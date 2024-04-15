import numpy as np
import itertools
import pyarrow.parquet as pq

from map_raster_cells_to_s2 import get_s2_from_raster_cells, save_mappings_parquet
from multiprocessing import Pool, cpu_count

from misc.misc_timer import timer
from misc.LogFile import LogFile


@timer
def parallel_get_s2_from_raster_cells(raster_cells, resolution, num_chunks, num_processes):
    chunks_raster_cells = np.array_split(raster_cells, num_chunks)

    pool = Pool(processes=num_processes)

    results = [pool.apply_async(get_s2_from_raster_cells, args=(corner_array, resolution)) for corner_array in chunks_raster_cells]

    pool.close()
    pool.join()

    s2 = np.concatenate([result.get() for result in results])

    return s2


def run_parallel_map_raster_cells_to_s2(resolution, log_path, parquet_file, batch_size, save_folder):
    log = LogFile(log_file=log_path)

    file = pq.ParquetFile(parquet_file)
    batches = file.iter_batches(batch_size=batch_size)

    for batch in itertools.islice(batches, log.get_log_idx(), None):
        raster_cells = np.asarray(batch.to_pandas())

        s2_cells = parallel_get_s2_from_raster_cells(raster_cells, resolution=resolution, num_chunks=cpu_count() * 10,
                                                     num_processes=cpu_count())

        save_mappings_parquet(f"{save_folder}raster_cells_s2_mappings_{log.get_log_idx()}.parquet", raster_cells,
                              s2_cells)

        log.update_log_idx(log.get_log_idx() + 1)


if __name__ == "__main__":
    resolution = 24
    log_path = "/projects/mdm/S2Mappings/logging/s2_mappings_logging_idx.txt"
    # Requires the unique_raster_cells.parquet file to be created (run misc/unique_raster_cells.py)
    parquet_file = "/projects/mdm/S2Mappings/unique_raster_cells.parquet"
    batch_size = 1_000_000
    save_folder = "/projects/mdm/S2Mappings/corner_mappings/"

    run_parallel_map_raster_cells_to_s2(resolution, log_path, parquet_file, batch_size, save_folder)
