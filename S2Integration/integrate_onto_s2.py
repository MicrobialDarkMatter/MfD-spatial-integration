import numpy as np
import pyarrow.parquet as pq
import pyarrow

from functools import partial

import itertools

from babelgrid import Babel

from multiprocessing import Pool, cpu_count

from map_raster_cells_to_s2 import change_mappings_lon_lat, get_cell_polygons


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


def poly_to_s2_id(poly, grid, resolution):
    centroid = poly.centroid
    s2_id = grid.geo_to_tile(lat=centroid.x, lon=centroid.y, resolution=resolution).tile_id

    return convert_s2_id_to_bit_id(s2_id)


# TODO: FIX below
S2_GRID = Babel("S2")
RESOLUTION = 16
poly_to_s2_id = np.vectorize(partial(poly_to_s2_id, grid=S2_GRID, resolution=RESOLUTION))


def run_raster_cell_to_s2(batch):
    raster_cells = np.array(batch)

    raster_cells = change_mappings_lon_lat(raster_cells)
    poly = get_cell_polygons(raster_cells)
    s2_ids = poly_to_s2_id(poly)

    return s2_ids


def parallel_get_high_level_s2_cells(batches, num_processes):
    pool = Pool(processes=num_processes)

    results = [pool.apply_async(run_raster_cell_to_s2, args=(batch,)) for batch in batches]

    pool.close()
    pool.join()

    s2 = np.concatenate([result.get() for result in results])

    return s2


if __name__ == "__main__":
    parquet_file = "/projects/mdm/S2Mappings/unique_raster_cells.parquet"

    file = pq.ParquetFile(parquet_file)
    batches = file.iter_batches(batch_size=1_000_000)

    s2_cells = parallel_get_high_level_s2_cells(batches, num_processes=cpu_count())
    print(s2_cells.shape)

    table = file.read()
    table = table.append_column("s2_cells", pyarrow.array(s2_cells))

    pq.write_table(table, "/projects/mdm/S2Mappings/unique_raster_cells_with_s2_cells.parquet")
