import geopandas
import geopandas as gpd
import pandas as pd
from osgeo import ogr,gdal
import numpy 
from shapely.wkb import loads
import pygeos
import matplotlib.pyplot as plt
import openpyxl
import itertools
import networkx as nx
from simplify import *

import warnings
warnings.filterwarnings("ignore")


# Create coordinates of random start and end points in the research area
def create_grid(bbox, height):
    """Create a vector-based grid.

    Args:
        bbox: A tuple or list containing the bounding box coordinates as (xmin, ymin, xmax, ymax).
        height: The size of the grid.

    Returns:
        A PyGEOS polygon object representing the grid.

    """

    # Set xmin, ymin, xmax, and ymax of the grid
    xmin, ymin = pygeos.total_bounds(bbox)[0], pygeos.total_bounds(bbox)[1]
    xmax, ymax = pygeos.total_bounds(bbox)[2], pygeos.total_bounds(bbox)[3]
    
    # Estimate total rows and columns
    rows = int(np.ceil((ymax-ymin) / height))
    cols = int(np.ceil((xmax-xmin) / height))

    # Set corner points
    x_left_origin = xmin
    x_right_origin = xmin + height
    y_top_origin = ymax
    y_bottom_origin = ymax - height

    # Create actual grid
    res_geoms = []
    for countcols in range(cols):
        y_top = y_top_origin
        y_bottom = y_bottom_origin
        for countrows in range(rows):
            # Append the four corner points of each cell to the list
            res_geoms.append((
                ((x_left_origin, y_top), (x_right_origin, y_top),
                 (x_right_origin, y_bottom), (x_left_origin, y_bottom)
                )))
            y_top = y_top - height
            y_bottom = y_bottom - height
        x_left_origin = x_left_origin + height
        x_right_origin = x_right_origin + height

    # Convert the list of corner points into a PyGEOS polygon object
    return pygeos.polygons(res_geoms)

def coordinates_pairs(create_grid):
    """
    Generates a pandas DataFrame of all possible coordinate pairs between grid center points.

    Args:
    - create_grid: a GeoDataFrame containing the grid polygons, with a 'geometry' column representing their coordinates as PyGEOS Polygon objects.

    Returns:
    - A pandas DataFrame containing all possible coordinate pairs as PyGEOS Point objects. 
    - The DataFrame has two columns: 's_coordinates' and 'e_coordinates', which represent the starting and ending coordinates of each pair, respectively.

    Example:
    >>> import geopandas as gpd
    >>> import pandas as pd
    >>> import pygeos
    >>> import itertools
    >>> create_grid = gpd.read_file('grid.shp')
    >>> coord_pairs = coordinates_pairs(create_grid)

    Note: This function requires the geopandas, pandas, pygeos, and itertools libraries to be installed.
    """
    # Calculate the centroid of each grid polygon and store them in a new GeoDataFrame
    grid_center_coordinates = gpd.GeoDataFrame(pygeos.centroid(create_grid.geometry.values),columns=['geometry'])
    
    # Create a new GeoDataFrame containing all possible pairs of coordinates from the grid centroids
    coordinates_pairs = gpd.GeoDataFrame(itertools.permutations(grid_center_coordinates.geometry, 2), columns=['s_coordinates', 'e_coordinates'])
    
    # Convert the GeoDataFrame to a pandas DataFrame and return a copy of the DataFrame
    return pd.DataFrame(coordinates_pairs.copy())