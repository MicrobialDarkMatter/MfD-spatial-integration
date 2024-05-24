import sys
sys.path.append("/projects/mdm/MfD-spatial-integration/")

from spatial_parallel import raster_to_rdf_parallel, raster_values_to_rdf


if __name__ == "__main__":
    print("mappings")
    save_path = "/home/cs.aau.dk/cp68wp/triples/values/"
    parquet_folder = "/projects/mdm/S2Mappings/raster_cells/"
    raster_to_rdf_parallel(raster_values_to_rdf, parquet_folder, save_path)
