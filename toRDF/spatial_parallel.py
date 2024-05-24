import sys
sys.path.append("/projects/mdm/MfD-spatial-integration/")

from spatial_to_rdf import *

from multiprocessing import Pool, cpu_count

from S2Integration.misc.misc_timer import timer


def raster_values_to_rdf(parquet_folder, save_path, file):
    save_file = os.path.join(save_path, file.split(".")[0] + ".nt.gz")

    df = pq.read_table(os.path.join(parquet_folder, file)).to_pandas()

    G = Graph()  # Initialize an empty graph

    file_name = file.removesuffix(".parquet")

    file_observation = file_name.split(".")[0]
    file_observation = remove_suffix_numbering(file_observation)

    G.add((URIRef(MFD + file_name), URIRef(RDF.type), URIRef(MFD + "RasterFile")))

    for r in df.itertuples(index=False):  # for row in dataframe
        raster_cell = get_raster_cell_id(r)

        measurement = URIRef(MFD + file_name + "_" + raster_cell)

        # RasterFile hasMember RasterCell
        G.add((URIRef(MFD + file_name), URIRef(OBOE + "hasMember"), URIRef(MFD + raster_cell)))

        # RasterCell hasMeasurement Measurement
        G.add((URIRef(MFD + raster_cell), URIRef(OBOE + "hasMeasurement"), measurement))

        G.add((URIRef(MFD + raster_cell), URIRef(RDF.type), URIRef(MFD + "RasterCell")))

        # Measurement hasValue literal
        lit, lit_type = get_literal(r[8])
        G.add((measurement, URIRef(OBOE + "hasValue"), Literal(lit, datatype=lit_type)))

        # Measurement Type Measurement
        G.add((measurement, URIRef(RDF.type), URIRef(OBOE + "Measurement")))

        # Measurement ContainsMeasurementsOfType MeasurementType
        G.add((measurement, URIRef(OBOE + "containsMeasurementsOfType"), URIRef(MFD + file_observation)))

        # MeasurementType Type MeasurementType
        G.add((URIRef(MFD + file_observation), URIRef(RDF.type), URIRef(OBOE + "MeasurementType")))

    with gzip.open(save_file, mode="wt", encoding="utf-8") as triple_file:
        triple_file.write(G.serialize(format='nt'))


def raster_mappings_to_rdf(parquet_folder, save_path, file):
    save_file = os.path.join(save_path, file.split(".")[0] + ".nt.gz")

    G = Graph()  # Initialize an empty graph

    df = pq.ParquetDataset(os.path.join(parquet_folder, file)).read().to_pandas()
    for r in df.iterrows():
        s2_cells = [convert_s2_id_to_bit_id(hex_id) for hex_id in r[1].iloc[8]]

        raster_cell = get_raster_cell_id(list(r[1]))

        for s2_cell in s2_cells:
            # RasterCell Covers S2Cell
            G.add(triple=(URIRef(MFD + raster_cell),
                          URIRef(GEO + "ehCovers"),
                          URIRef(KWG_ONT + s2_cell)))

    with gzip.open(filename=save_file, mode="at", encoding="utf-8") as triple_file:
        triple_file.write(G.serialize(format='nt'))


@timer
def raster_to_rdf_parallel(function, parquet_folder, save_path):
    files = [file for file in os.listdir(parquet_folder) if file.endswith('.parquet')]

    # Create a pool of workers
    with Pool(cpu_count()) as pool:
        # Process files in parallel
        pool.starmap(function, [(parquet_folder, save_path, file) for file in files])


if __name__ == "__main__":
    # print("values")
    # save_path = "/home/cs.aau.dk/cp68wp/triples/values"
    # parquet_folder = "/projects/mdm/teaserMappings/raster_cells/"
    # raster_to_rdf_parallel(raster_values_to_rdf, parquet_folder, save_path)

    print("mappings")
    save_path = "/home/cs.aau.dk/cp68wp/triples/mappings"
    parquet_folder = "/projects/mdm/teaserMappings/corner_mappings/"
    raster_to_rdf_parallel(raster_mappings_to_rdf, parquet_folder, save_path)
