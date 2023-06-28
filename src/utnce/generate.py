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
    
    replacement_dict = {
    'Amsterdam Centraal': 'Centraal Station',
    }

    am_sub_routes['to'] = am_sub_routes['to'].replace(replacement_dict, regex=True).str.strip()
    am_sub_routes = am_sub_routes.reset_index(drop=True)

    am_sub_start_station_name_dict = start_station_dict(am_sub_routes)

    am_sub_line_dict = line_dict(am_sub_routes)

    am_all_sub_stations_name = all_station_list(am_all_sub_stations)

    am_sub_order_route_dict = order_stations_inline(am_sub_line_dict,am_all_sub_stations_name,am_sub_routes,am_sub_start_station_name_dict)

    am_sub_order_id_pairs = id_pairs_inline(am_sub_line_dict,am_sub_order_route_dict)

    G = create_ground_graph(edges, nodes)


    am_sub_shortest_path_pairs = am_sub_order_id_pairs.copy()
    duplicate_row_count = am_sub_order_id_pairs.copy()
    am_sub_shortest_path_edges = am_sub_order_id_pairs.copy()
    am_sub_edges = am_sub_order_id_pairs.copy()

    for line in am_sub_order_id_pairs.keys():
        am_sub_shortest_path_pairs[line] = all_shortest_paths(am_sub_order_id_pairs[line],edges)
        duplicate_row_count[line], am_sub_shortest_path_edges[line], am_sub_edges[line] = edges_with_count_weight(am_sub_shortest_path_pairs[line],edges)

    plot_routes_even(am_sub_routes,edges,am_sub_shortest_path_edges)

    plot_routes_odd(am_sub_routes,edges,am_sub_shortest_path_edges)

    
def tram():




def bus():




if __name__ == "__main__":

    metro(sys.argv[1])
    