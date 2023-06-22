import geopandas
import geopandas as gpd
import pandas as pd
from osgeo import ogr,gdal
import numpy 
from shapely.wkb import loads
from shapely.geometry import Point
from shapely.geometry import LineString
import matplotlib.pyplot as plt
import openpyxl
import itertools
import networkx as nx
from simplify import *
from prepare import *

import warnings
warnings.filterwarnings("ignore")

def tram():

def metro(osm_path,city='Rotterdam'):
    """
    Builds a metro network from OpenStreetMap data.
    
    Parameters:
        osm_path (str): The path to the OpenStreetMap data.
        city (str) : The name of the city.
        
    Returns:
        
    """

    # Extract subway stations
    am_all_sub_stations = sub_stations(osm_path)

    # Extract subway network
    sub = subway_network(osm_path)

    # Prepare network edges and nodes
    edges, nodes = prepare_network(sub, am_all_sub_stations)

    # Expand edges
    edges = expand_edges(edges)

    # Extract subway routes
    city_sub_routes = sub_routes(osm_path)

    # Sort subway routes
    am_sub_routes = sorted_routes(am_sub_routes)

    # Check subway routes against subway stations
    check_name = check_to_column(am_sub_routes, am_all_sub_stations)
    

def bus():


if __name__ == "__main__":

    metro(sys.argv[1])