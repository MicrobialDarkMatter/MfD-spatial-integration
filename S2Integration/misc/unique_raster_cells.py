import numpy as np
import pyarrow.parquet as pq
import os


def read_parquet(path: str, with_data=False) -> np.array:
    if with_data:
        table = pq.read_table(path, columns=["ll_easting", "ll_northing", "ul_easting", "ul_northing",
                                             "ur_easting", "ur_northing", "lr_easting", "lr_northing",
                                             "data"])
    else:
        table = pq.read_table(path, columns=["ll_easting", "ll_northing", "ul_easting", "ul_northing",
                                             "ur_easting", "ur_northing", "lr_easting", "lr_northing"])

    return np.asarray(table)


def get_unique_raster_cells(raster_cell_folder: str) -> np.array:
    """
    Get all unique raster_cells from a folder of parquet raster_cells.

    Args:
        raster_cell_folder (str): Folder to parquet raster_cells

    Returns: All unique raster_cells

    """
    raster_cell_paths = [os.path.join(raster_cell_folder, file) for file in os.listdir(raster_cell_folder)]

    unique_raster_cells = None

    first = True
    for raster_cell_path in raster_cell_paths:
        raster_cell = read_parquet(raster_cell_path)
        if first:
            unique_raster_cells = raster_cell
            first = False
        else:
            unique_raster_cells = np.unique(np.concatenate([unique_raster_cells, raster_cell]), axis=0)

    return unique_raster_cells


if __name__ == "__main__":
    import pyarrow as pa
    raster_cell_folder = '/projects/mdm/S2Mappings/raster_cells'

    raster_cells = get_unique_raster_cells(raster_cell_folder)

    raster_cells = pa.table({
        "ll_easting": raster_cells[:, 0],
        "ll_northing": raster_cells[:, 1],
        "ul_easting": raster_cells[:, 2],
        "ul_northing": raster_cells[:, 3],
        "ur_easting": raster_cells[:, 4],
        "ur_northing": raster_cells[:, 5],
        "lr_easting": raster_cells[:, 6],
        "lr_northing": raster_cells[:, 7]
    })

    save_path = os.path.join("/projects/mdm/S2Mappings/unique_raster_cells.parquet")

    pq.write_table(raster_cells, save_path)
    print(raster_cells.shape)
