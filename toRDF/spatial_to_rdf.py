import numpy as np
import polars as pl
import re

import os
from rdflib import Graph, URIRef, Literal, BNode
# from rdflib.namespace import RDF, XSD

from variables import RDF, RDFS, XSD, MFD, OBOE, GEO, KWG_ONT

import gzip
import pyarrow.parquet as pq

from TBox import create_tbox


remove_suffix_numbering = lambda x: re.sub(r'[_\d]+$', "", x)

def convert_s2_id_to_bit_id(s2_hex):
    """
    Converts a given S2 ID from hexadecimal format to binary format.

    Args:
        s2_hex (str): The S2 ID in hexadecimal format.

    Returns:
        str: The S2 ID converted to binary format with a length of 64 characters.
    """
    # Adds 0's at the front until length of id is 16
    s2_id = s2_hex.ljust(16, "0")

    # Converts Hex to Bit and adds trailing 0's until length is 64
    bit_id = np.base_repr(int(s2_id, 16), base=2).rjust(64, "0")
    return bit_id


def get_raster_cell_id(r):
    """
    Returns the raster cell ID for a given row in a raster. A raster cell ID is the combination of the cell's four corners (4 corners x 2 dimensions = 8 entries)
    i.e., ll_easting, ll_northing, ul_easting, ul_northing, ur_easting, ur_northing, lr_easting, lr_northing

    Args:
        r (tuple): A row in the mappings file.

    Returns:
        str: The raster cell ID.
    """
    return f"{int(r[0])}_{int(r[1])}_{int(r[2])}_{int(r[3])}_{int(r[4])}_{int(r[5])}_{int(r[6])}_{int(r[7])}"


def get_literal(literal):
    """
    Returns a tuple containing the literal value and its corresponding XSD data type.

    Parameters:
        literal (str, int, float, bool): The literal value to be processed.

    Returns:
        tuple: A tuple containing the literal value and its corresponding XSD data type.
    """
    if isinstance(literal, str):
        object_type = XSD.string
    elif isinstance(literal, int):
        object_type = XSD.integer
    elif isinstance(literal, float):
        object_type = XSD.decimal
    elif isinstance(literal, bool):
        object_type = XSD.boolean
    else:
        print(f"Instance unknown for literal: {literal}. Type: {type(literal)}.")
        object_type = XSD.string

    return literal, object_type


def raster_values_to_rdf(parquet_folder, save_file):
    """
    Converts the raster values to RDF format.
    """
    # Open the file to write the triples
    with gzip.open(filename=save_file, mode="at", encoding="utf-8") as triple_file:
        # Iterate over the parquet files in the folder
        for file in os.listdir(parquet_folder):
            df = pl.read_parquet(parquet_folder + file)

            G = Graph()  # Initialize an empty graph

            file_name = file.removesuffix(".parquet")

            file_observation = file_name.split(".")[0]
            file_observation = remove_suffix_numbering(file_observation)

            G.add(triple=(URIRef(MFD + file_name),
                          URIRef(RDF.type),
                          URIRef(MFD + "RasterFile")))

            for r in df.iter_rows():  # for row in dataframe - r[0] value of row in column 1, r[1] value of row in column 2, etc.
                raster_cell = get_raster_cell_id(r)

                # measurement = BNode()
                measurement = URIRef(MFD + file_name + "_" + raster_cell)

                # RasterFile hasMember RasterCell
                G.add(triple=(URIRef(MFD + file_name),
                              URIRef(OBOE + "hasMember"),
                              URIRef(MFD + raster_cell)))

                # RasterCell hasMeasurement Measurement
                G.add(triple=(URIRef(MFD + raster_cell),
                              URIRef(OBOE + "hasMeasurement"),
                              measurement))

                G.add(triple=(URIRef(MFD + raster_cell),
                              URIRef(RDF.type),
                              URIRef(MFD + "RasterCell")))

                # Measurement hasValue literal
                lit, lit_type = get_literal(r[8])
                G.add(triple=(measurement,
                              URIRef(OBOE + "hasValue"),
                              Literal(lit, datatype=lit_type)))

                # Measurement Type Measurement
                G.add(triple=(measurement,
                              URIRef(RDF.type),
                              URIRef(OBOE + "Measurement")))

                # Measurement ContainsMeasurementsOfType MeasurementType
                G.add(triple=(measurement,
                              URIRef(OBOE + "containsMeasurementsOfType"),
                              URIRef(MFD + file_observation)))

                # MeasurementType Type MeasurementType
                G.add(triple=(URIRef(MFD + file_observation),
                              URIRef(RDF.type),
                              URIRef(OBOE + "MeasurementType")))

            triple_file.write(G.serialize(format='nt'))


def raster_mappings_to_rdf(parquet_datasets_path_or_folder, save_file):
    """
    Converts the raster mappings to RDF format.
    """
    with gzip.open(filename=save_file, mode="at", encoding="utf-8") as triple_file:
        df = pq.ParquetDataset(parquet_datasets_path_or_folder).read().to_pandas()
        for r in df.iterrows():
            G = Graph()  # Initialize an empty graph
            s2_cells = [convert_s2_id_to_bit_id(hex_id) for hex_id in r[1].iloc[8]]

            raster_cell = get_raster_cell_id(list(r[1]))

            for s2_cell in s2_cells:
                # TODO: Add to TBOX
                # # S2Cell Type KWG:S2Cell
                # G.add(triple=(URIRef(KWG_ONT + s2_cell),
                #               URIRef(RDF.type),
                #               URIRef(KWG_ONT + "S2Cell")))

                # RasterCell Covers S2Cell
                G.add(triple=(URIRef(MFD + raster_cell),
                              URIRef(GEO + "ehCovers"),
                              URIRef(KWG_ONT + s2_cell)))

            triple_file.write(G.serialize(format='nt'))


if __name__ == "__main__":
    create_tbox(save_file="tbox.nt.gz")

    raster_values_to_rdf(parquet_folder="/projects/mdm/S2Mappings/raster_cells/",
                         save_file="raster_values.nt.gz")

    raster_mappings_to_rdf(parquet_datasets_path_or_folder="/projects/mdm/S2Mappings/corner_mappings/",
                           save_file="raster_s2_mappings.nt.gz")
