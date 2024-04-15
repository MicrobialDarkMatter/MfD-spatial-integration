import numpy as np
from babelgrid import Babel
from shapely.geometry import Polygon, Point

from misc.CoordinateTransformer import CoordinateTransformer
from misc.unique_raster_cells import get_unique_raster_cells

import pyarrow as pa
import pyarrow.parquet as pq


def map_mappings(row, mappings):
    key = tuple(row)
    return mappings[key]


def change_mappings_lon_lat(raster_cells):
    """
    Change raster_cells from UTM to lon, lat
    Args:
        raster_cells: Np of unique raster_cells with

    Returns: raster_cells as lon, lat in np array (x, 8)

    """
    raster_cells_shape = raster_cells.shape

    # Each unique corner from raster_cells
    coordinates = np.unique(raster_cells.reshape(-1, 2), axis=0)

    coordinate_transformer = CoordinateTransformer()
    lon_lat = np.apply_along_axis(coordinate_transformer.transform_coordinates, axis=1, arr=coordinates)

    coordinate_mappings = {tuple(key): tuple(val) for key, val in zip(coordinates, lon_lat)}

    # Apply coordinate mapping on each corner
    coordinates_lon_lat = np.apply_along_axis(map_mappings, axis=1, arr=raster_cells.reshape(-1, 2),
                                              mappings=coordinate_mappings)
    # Return reshaped coordinates, to get raster_cells back from individual corners
    return coordinates_lon_lat.reshape(raster_cells_shape)


def create_polygon(bounds: list[tuple]) -> Polygon:
    """
    Takes as input a list of coordinates that defines the convex hull of a polygon and creates that polygon as a Python object (shapely.Polygon).

    Args:
        bounds (list[tuple]): A list of tuples defining the convex hull [(x1, y1), (x2, y2), ...].

    Returns:
        Polygon: The Python object created from the coordinates specifying its convex hull.
    """

    latitudes = [bound[1] for bound in bounds]
    longitudes = [bound[0] for bound in bounds]

    polygon_geometry = Polygon(zip(latitudes, longitudes))

    return polygon_geometry


def majority_rule_on_tiles(S2_cells, poly):
    if isinstance(S2_cells, type(np.NaN)):
        return np.NaN
    centroids = [tile.geometry.centroid for tile in S2_cells]  # Centroids, (lat, long)
    contained = [poly.contains(Point(x, y)) for y, x in centroids]  # Bool if contained

    S2_cells = [tile for tile, contain in zip(S2_cells, contained) if contain]  # Remove outside S2_cells

    return S2_cells


def get_cell_polygons(raster_cells):
    polygons = np.array(
        [create_polygon([(x[0], x[1]), (x[2], x[3]), (x[4], x[5]), (x[6], x[7])]) for x in raster_cells],
        dtype=object
    )

    return polygons


def get_s2_cells_from_polygons(polygons, resolution):
    grid = Babel('s2')
    s2_cells = np.array([grid.polyfill(x, resolution=resolution) for x in polygons], dtype=object)

    return s2_cells


def update_s2_cells_majority_rule(s2_cells, polygons):
    s2_cells = np.array([majority_rule_on_tiles(s2, poly)
                         for s2, poly in zip(s2_cells, polygons)],
                        dtype=object)

    return s2_cells


def get_s2_tile_ids(s2_cells):
    get_tiles = lambda x: tuple(item.tile_id for item in x)

    s2_cells = np.array([get_tiles(row) for row in s2_cells], dtype=object)

    return s2_cells


def save_mappings_parquet(save_buffer, raster_cells, s2_cells):
    mappings = pa.table({
        "ll_easting": raster_cells[:, 0],
        "ll_northing": raster_cells[:, 1],
        "ul_easting": raster_cells[:, 2],
        "ul_northing": raster_cells[:, 3],
        "ur_easting": raster_cells[:, 4],
        "ur_northing": raster_cells[:, 5],
        "lr_easting": raster_cells[:, 6],
        "lr_northing": raster_cells[:, 7],
        "s2_cells": s2_cells,
    })

    pq.write_table(mappings, save_buffer)


def get_s2_from_raster_cells(raster_cells, resolution):
    raster_cells_lon_lat = change_mappings_lon_lat(raster_cells)

    polygons = get_cell_polygons(raster_cells_lon_lat)

    s2_cells = get_s2_cells_from_polygons(polygons, resolution=resolution)

    s2_cells = update_s2_cells_majority_rule(s2_cells, polygons)

    s2_cells = get_s2_tile_ids(s2_cells)

    return s2_cells


if __name__ == "__main__":
    raster_cell_folder = '/projects/mdm/MFDxEnvironmentalData/raster_cells'

    raster_cells = get_unique_raster_cells(raster_cell_folder)

    raster_cells = raster_cells[:1000, :]

    s2 = get_s2_from_raster_cells(raster_cells, resolution=24)
    print(s2.shape)

    save_mappings_parquet('/projects/mdm/MFDxEnvironmentalData/raster_cells_s2_mappings.parquet', raster_cells, s2)

