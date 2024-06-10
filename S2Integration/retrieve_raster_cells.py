import rasterio

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

import os


def get_windows(num_chunks: int, file_path: str) -> ...:
    """_summary_

    Args:
        num_chunks (int): _description_
        file_path (str): _description_

    Returns:
        _type_: _description_
    """
    with rasterio.open(file_path) as src:
        total_width = src.width
        total_height = src.height

    chunk_width = total_width // num_chunks
    windows = []
    for col_off in range(0, total_width, chunk_width):
        # Calculate the ending column for the current chunk
        end_col = min(col_off + chunk_width, total_width)

        # Define the window for the current chunk
        window = rasterio.windows.Window(col_off=col_off, row_off=0, width=end_col - col_off, height=total_height)
        windows.append(window)

    return windows


def get_corner(window: rasterio.windows.Window, xy_transform: rasterio.Affine, offset: str = "ul" or "ur" or "ll" or "lr") -> np.array:
    """Takes as input (a subset of) the full raster (window) and returns the corners of each of the raster cells in UTM coordinates.
    In the current implementation, it is only run with offset="ul" - the rest of the corners are found using np.roll to offset the coordinates.

    Args:
        window (_type_): _description_
        xy_transform (_type_): _description_
        offset (str, optional): _description_. Can only be either "ul" or "ur" or "ll" or "lr".

    Returns:
        np.array: _description_
    """
    # Creates two meshgrids for lon and lat coordinates
    cols, rows = np.meshgrid(np.arange(window.col_off, window.col_off + window.width + 1),
                             np.arange(window.row_off, window.row_off + window.height + 1))
    
    easting, northing = rasterio.transform.xy(xy_transform, rows, cols, offset=offset)

    # Returns np.array with shape (width, height, 2) – 2 for lon and lat
    return np.dstack([easting, northing]).astype(np.int32)


def get_raster_cells(raster_path: str, window: rasterio.windows.Window=None) -> np.array:
    """
    Get all raster_cells and the measurements for a raster file in the UTM format

    Args:
        raster_path (str): Path to raster
        window (_type_): If not None, the window to read from the raster. If None, reads the entire raster file.

    Returns:
        np.array: Corners in np.array(width*height, 8+1) [8 for corners and 1 for measurement]

    """
    with rasterio.open(raster_path) as src:
        xy_transform = src.transform
        if not window:
            window = rasterio.windows.Window(col_off=0, row_off=0, width=src.width, height=src.height)

        data = src.read(1, window=window)  # Fix 1 if more rasters. Then shape becomes (N, lon/lat, lat/lon)
        nodata = src.nodata

        if np.isnan(nodata):
            contains_data = ~np.isnan(data)
        else:
            contains_data = data != nodata

    ul_corner = get_corner(window=window, xy_transform=xy_transform, offset="ul")

    # Shift ul (UpperLeft) corner to get bl, ur, and br corners. (BottomLeft, UpperRight, and BottomRight)
    ll_corners_full = np.roll(ul_corner, -1, axis=0)
    ur_corners_full = np.roll(ul_corner, -1, axis=1)
    lr_corners_full = np.roll(ur_corners_full, -1, axis=0)

    # Excluding edges used for calculations
    raster_cells = np.concatenate((ll_corners_full[:-1, :-1, :],
                                   ul_corner[:-1, :-1, :],
                                   ur_corners_full[:-1, :-1, :],
                                   lr_corners_full[:-1, :-1, :]),
                                   axis=2)

    # Adding data to corners
    raster_cells = np.dstack([raster_cells, data])

    # Remove no data
    raster_cells = raster_cells.reshape((-1, 8 + 1))[contains_data.flatten()]

    return raster_cells


# def get_file_name(raster_path: str) -> str:
#     name = os.path.basename(raster_path)#.removesuffix(".tif").removesuffix(".vrt")
#     return name


def save_raster_cells_parquet(raster_cells: np.array, save_folder: str, raster_path: str) -> None:
    """_summary_

    Args:
        raster_cells (np.array): The output of get_raster_cells()
        save_folder (str): _description_
        raster_path (str): _description_
    """
    raster_cells = pa.table({
        "ll_easting": raster_cells[:, 0],
        "ll_northing": raster_cells[:, 1],
        "ul_easting": raster_cells[:, 2],
        "ul_northing": raster_cells[:, 3],
        "ur_easting": raster_cells[:, 4],
        "ur_northing": raster_cells[:, 5],
        "lr_easting": raster_cells[:, 6],
        "lr_northing": raster_cells[:, 7],
        "data": raster_cells[:, 8],
    })

    save_path = os.path.join(save_folder, os.path.basename(raster_path) + ".parquet")

    pq.write_table(raster_cells, save_path)


def paths_from_folder(directory: str, file_format: str=".tif") -> list[str]:
    file_paths = []

    # Walk through the directory and its subdirectories
    for root, directories, files in os.walk(directory):
        for _file in files:
            if _file.endswith(file_format):
                file_paths.append(os.path.join(root, _file))

    return file_paths


if __name__ == "__main__":
    from misc.misc_timer import format_time

    import time

    # # # # ALL FILES SINGLE # # # #
    file_paths = paths_from_folder('/home/bio.aau.dk/wz65bi/mfd_sdm/data/download/EcoDes-DK15/', file_format=".vrt") + \
                 paths_from_folder('projects/mdm/functional_maps/', file_format=".tif")

    # file_paths = ['/projects/mdm/functional_maps/Clay.tif']

    start_time = time.time()
    for file_path in file_paths:
        print(file_path)
        raster_cells = get_raster_cells(raster_path=file_path)

        save_raster_cells_parquet(raster_cells,
                            save_folder='/projects/mdm/S2Mappings/raster_cells',
                            raster_path=file_path
                            )

    end_time = time.time()
    print(f"Saved Cells Parquets @ {format_time(start_time, end_time)}")

    # parquets = [file.removesuffix(".parquet") for file in os.listdir('/projects/mdm/raster_pixels')]
    # file_paths = [file_path for file_path in file_paths if
    #               os.path.basename(file_path).removesuffix(".tif") not in parquets]
