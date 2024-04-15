import pandas as pd
from babelgrid import Babel

from map_points_to_s2 import get_coordinates_columns, points_to_s2, save_point_mappings
from retrieve_raster_cells import save_raster_cells_parquet, get_raster_cells
from parallel_map_raster_cells_to_s2 import parallel_get_s2_from_raster_cells, cpu_count, save_mappings_parquet
from misc.unique_raster_cells import get_unique_raster_cells


def main(raster_file_paths, raster_cell_folder, mappings_file_path, S2_level, points_path, lat_col, lon_col, grid, points_mappings_path, points_errors_path):
    # # # # Get MfD Mappings # # # #
    points = get_coordinates_columns(points=pd.read_excel(points_path),
                                     lat_col=lat_col,
                                     lon_col=lon_col)
    
    mapped_points = points_to_s2(points=points,
                                 grid=grid,
                                 grid_resolution=S2_level)
    
    mapping_errors = mapped_points[mapped_points['s2_cell_id'].isnull()]
    
    save_point_mappings(mapped_points, points_mappings_path)
    save_point_mappings(mapping_errors, points_errors_path)

    # # # # Retrieve Raster Corners # # # #
    for file_path in raster_file_paths:
        raster_cells = get_raster_cells(raster_path=file_path)

        save_raster_cells_parquet(raster_cells,
                                  save_folder=raster_cell_folder,
                                  raster_path=file_path)

    # # # Map Corners to S2 # # # #
    raster_cells = get_unique_raster_cells(raster_cell_folder)

    s2_cells = parallel_get_s2_from_raster_cells(raster_cells, resolution=S2_level, num_chunks=cpu_count() * 10,
                                                 num_processes=cpu_count())

    save_mappings_parquet(mappings_file_path, raster_cells, s2_cells)


if __name__ == "__main__":
    # Specify the following general arguments
    S2_level = 20
    grid = Babel('S2')

    # Specify the following arguments for mapping points to the S2 Geometry
    points_path = '../example/data/mfd_points.xlsx'
    points_mappings_path = '../example/output/mfd_mappings.parquet'
    points_errors_path = '../example/output/mfd_mappings_errors.parquet'
    lat_col = 'latitude'
    lon_col = 'longitude'

    # Specify the following arguments for mapping raster files to the S2 Geometry
    raster_file_paths = ["../example/data/solar_radiation.vrt", "../example/data/slope.tif"]
    raster_cell_folder = '../example/output/raster_corners/'
    mappings_file_path = '../example/output/raster_s2_mappings.parquet'

    main(raster_file_paths=raster_file_paths,
         raster_cell_folder=raster_cell_folder,
         mappings_file_path=mappings_file_path,
         S2_level=S2_level,
         points_path=points_path,
         lat_col=lat_col,
         lon_col=lon_col,
         grid=grid,
         points_mappings_path=points_mappings_path,
         points_errors_path=points_errors_path)
    
