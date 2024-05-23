import multiprocessing as mp
import time

import numpy as np
import polars as pl
import re

import os
from rdflib import Graph, URIRef, Literal, BNode
# from rdflib.namespace import RDF, XSD

from variables import RDF, RDFS, XSD, MFD, OBOE, GEO, KWG_ONT

import gzip
import pyarrow.parquet as pq

from spatial_to_rdf import *


def write_to_file(write):
    with open("file.txt", "at") as f:
        f.write(write + "\n")


def raster_values_worker(file, q):
    # write_to_file(f"Reading {file}")

    df = pl.read_parquet(file)

    # write_to_file(f"Read {file}")

    G = Graph()  # Initialize an empty graph

    file_name = file.removesuffix(".parquet")

    file_observation = file_name.split(".")[0]
    file_observation = remove_suffix_numbering(file_observation)

    G.add(triple=(URIRef(MFD + file_name),
                  URIRef(RDF.type),
                  URIRef(MFD + "RasterFile")))
    q.put(G)

    for r in df.iter_rows():  # for row in dataframe - r[0] value of row in column 1, r[1] value of row in column 2, etc.
        G = Graph()  # Initialize an empty graph

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

    # write_to_file(f"Completed {file}")

        q.put(G)


def raster_mappings_worker(file, q):
    df = pq.ParquetDataset(file).read().to_pandas()

    for r in df.iterrows():
        G = Graph()

        s2_cells = [convert_s2_id_to_bit_id(hex_id) for hex_id in r[1].iloc[8]]

        raster_cell = get_raster_cell_id(list(r[1]))

        for s2_cell in s2_cells:
            # RasterCell Covers S2Cell
            G.add(triple=(URIRef(MFD + raster_cell),
                          URIRef(GEO + "ehCovers"),
                          URIRef(KWG_ONT + s2_cell)))

        q.put(G)


def listener(q, save_file):
    """listens for messages on the q, writes to file. """

    with gzip.open(filename=save_file, mode="at", encoding="utf-8") as triple_file:
        while True:
            G = q.get()
            if G == 'kill':
                break
            triple_file.write(G.serialize(format='nt'))
            triple_file.flush()


def raster_to_rdf_parallel(function, parquet_folder, save_file):
    # must use Manager queue here, or will not work
    manager = mp.Manager()
    q = manager.Queue()
    pool = mp.Pool(mp.cpu_count())

    # put listener to work first
    pool.apply_async(listener, (q, save_file))

    files = [os.path.join(parquet_folder, f) for f in os.listdir(parquet_folder) if f.endswith(".parquet")]

    # fire off workers
    jobs = []
    for file in files:
        job = pool.apply_async(function, (file, q))
        jobs.append(job)

    # collect results from the workers through the pool result queue
    for job in jobs:
        job.get()

    # now we are done, kill the listener
    q.put('kill')
    pool.close()
    pool.join()


if __name__ == "__main__":
    raster_to_rdf_parallel(function=raster_values_worker,
                           parquet_folder="/projects/mdm/teaserMappings/raster_cells/",  # TODO: Run on all mappings
                           save_file="/home/cs.aau.dk/cp68wp/raster_values_parallel.nt.gz")

    # raster_to_rdf_parallel(function=raster_mappings_worker,
    #                        parquet_folder="/projects/mdm/teaserMappings/corner_mappings/",
    #                        save_file="raster_s2_mappings_parallel.nt.gz")
