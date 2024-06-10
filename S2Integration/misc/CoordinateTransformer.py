import sys
sys.path.append("/projects/mdm/MfD-spatial-integration/")

import pyproj


class CoordinateTransformer:
    def __init__(self, prior_crs: str = "EPSG:25832", post_crs: str = "EPSG:4326") -> None:
        """
        Define the transformer by specifying which CRS that needs to be transformed, and which
        CRS it must be transformed to.

        Args:
            prior_crs (str, optional):  The CRS of the original coordinates.
                Defaults to "EPSG:25832" (METRES FROM REFERENCE MERIDIAN / FROM EQUATOR).
            post_crs (str, optional):   The desired CRS of the transformed coordinates.
                Defaults to "EPSG:4326" (LAT/LONG).
        """

        self.crs_transformer = pyproj.Transformer.from_crs(crs_from=prior_crs, crs_to=post_crs)

    def transform_coordinates(self, coordinate_pair: tuple[float, float]) -> tuple[float, float]:
        """
        Takes as input a coordinate pair (x, y) defined in the prior_crs Coordinate Reference System
        and transforms it to the corresponding representation in the post_crs Coordinate Reference System.

        Args:
            coordinate_pair (tuple[float, float]):  The coordinate pair in the original CRS (x, y)

        Returns:
            tuple[float, float]:    The coordinate pair in the new CRS (xt, yt)
        """

        return self.crs_transformer.transform(xx=coordinate_pair[0], yy=coordinate_pair[1])
