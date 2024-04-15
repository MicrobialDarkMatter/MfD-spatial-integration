import pandas as pd
from babelgrid import Babel


def get_coordinates_columns(points: pd.DataFrame, lat_col: str, lon_col: str) -> pd.DataFrame:
    """this function takes as input a dataframe of points and returns a dataframe with the latitude and longitude columns

    Args:
        points (pd.DataFrame): the input dataframe of points, possibly containing empty cells for the latitude and longitude

    Returns:
        pd.DataFrame: the input dataframe with only the latitude and longitude columns
    """
    
    coordinates = pd.DataFrame()
    coordinates[['latitude', 'longitude']] = points[[lat_col, lon_col]]
    
    return coordinates


def points_to_s2(points: pd.DataFrame, grid: Babel, grid_resolution: int) -> pd.DataFrame:
    """this function takes as input a dataframe of points and returns a dataframe with the S2 cell id for each point, determined using the apply method

    Args:
        points (pd.DataFrame): the input dataframe of points, possibly containing empty cells for the latitude and longitude
        grid_resolution (int): the desired resolution of the S2 grid

    Returns:
        pd.DataFrame: the input dataframe with an additional column containing the S2 cell id for each point
    """
    
    # create a new column with the S2 cell id for each point
    # each row contains (1) the latitude, (2) the longitude, and (3) the corresponding S2 cell id
    points['s2_cell_id'] = points.apply(lambda row: grid.geo_to_tile(lat=row['latitude'],
                                                                        lon=row['longitude'],
                                                                        resolution=grid_resolution).tile_id
                                                if pd.notnull(row['latitude']) and pd.notnull(row['longitude'])
                                                else None,
                                        axis=1)

    return points


def save_point_mappings(points: pd.DataFrame, save_path: str) -> None:
    """this function takes as input a dataframe of points and saves it to a parquet file

    Args:
        points (pd.DataFrame): the input dataframe of points, possibly containing empty cells for the latitude and longitude
        save_path (str): the path to the parquet file
    """
    
    points.to_parquet(path=save_path, index=True)
