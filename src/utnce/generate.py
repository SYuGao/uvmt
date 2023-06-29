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
from routing import *
from percolation_plot import *


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
    city_sub_stations = sub_stations(osm_path)

    # Extract subway network
    city_sub_network = subway_network(osm_path)

    # Prepare network edges and nodes
    edges, nodes = prepare_network(city_sub_network, city_sub_stations)

    # Expand edges
    edges = expand_edges(edges)

    # Extract subway routes
    city_sub_routes = sub_routes(osm_path)

    # Sort subway routes
    city_sub_routes = sorted_routes(city_sub_routes)

    # Check subway routes against subway stations
    check_name = check_to_column(city_sub_routes, city_sub_stations)
    
    replacement_dict = {
    'Amsterdam Centraal': 'Centraal Station',
    }

    city_sub_routes['to'] = city_sub_routes['to'].replace(replacement_dict, regex=True).str.strip()
    city_sub_routes = city_sub_routes.reset_index(drop=True)

    city_sub_start_station_name_dict = start_station_dict(city_sub_routes)

    city_sub_line_dict = line_dict(city_sub_routes)

    city_all_sub_stations_name = all_station_list(city_sub_stations)

    city_sub_order_route_dict = order_stations_inline(city_sub_line_dict, city_all_sub_stations_name, city_sub_stations, city_sub_start_station_name_dict)

    city_sub_order_id_pairs = id_pairs_inline(city_sub_line_dict, city_sub_order_route_dict)

    G = create_ground_graph(edges, nodes)


    city_sub_shortest_path_pairs = city_sub_order_id_pairs.copy()
    duplicate_row_count = city_sub_order_id_pairs.copy()
    city_sub_shortest_path_edges = city_sub_order_id_pairs.copy()
    city_sub_edges = citysub_order_id_pairs.copy()

    for line in city_sub_order_id_pairs.keys():
        city_sub_shortest_path_pairs[line] = all_shortest_paths(city_sub_order_id_pairs[line],edges)
        duplicate_row_count[line], city_sub_shortest_path_edges[line], city_sub_edges[line] = edges_with_count_weight(city_sub_shortest_path_pairs[line],edges)

    return plot_routes_even(city_sub_routes,edges, city_sub_shortest_path_edges), plot_routes_odd(city_sub_routes,edges, city_sub_shortest_path_edges), city_sub_shortest_path_pairs, city_sub_shortest_path_edges, city_sub_edges


def tram(osm_path,city='Rotterdam'):
    """
    Performs various operations related to trams.
    
    Parameters:
        osm_path (str): The path to the OpenStreetMap data.
        city (str) : The name of the city.
    
    Returns:
        tuple: A tuple containing multiple outputs related to trams.
    """
    # Retrieve tram stations from the OSM file
    city_tram_stations = tram_stations(osm_path)
    
    # Retrieve tram network information from the OSM file
    city_tram_network = tram_network(osm_path)
    
    # Prepare network edges and nodes based on tram stations and network information
    edges, nodes = prepare_network(city_tram_network, city_tram_stations)
    
    # Expand edges (if required)
    edges = expand_edges(edges)
    
    # Retrieve tram routes information from the OSM file
    city_tram_routes = tram_routes(osm_path)
    
    # Exclude tram routes with 'EMA' as the reference
    city_tram_routes = city_tram_routes.loc[city_tram_routes.ref != 'EMA']
    
    # Sort tram routes
    city_tram_routes = sorted_routes(city_tram_routes)
    
    # Check and revise tram routes and stations
    need_revised_row = check_to_column(city_tram_routes, city_tram_stations)
    
    # Define replacement dictionary for specific string replacements
    replacement_dict = {
        ',': '',
        'Diemen': '',
        'Amsterdam': '',
        'Sloterdijk': 'Station Sloterdijk',
        'Osdorp Dijkgraafsplein': 'Dijkgraafplein',
        'Osdorp De Aker': 'Matterhorn'
    }
    
    # Perform string replacements in the 'to' column of tram routes
    city_tram_routes['to'] = city_tram_routes['to'].replace(replacement_dict, regex=True).str.strip()
    city_tram_routes['to'] = city_tram_routes['to'].replace('Amstelveen Westwijk', 'Westwijk').str.strip()
    city_tram_routes = city_tram_routes.reset_index(drop=True)
    
    # Create dictionaries for start stations and tram lines
    city_tram_start_station_name_dict = start_station_dict(city_tram_routes)
    city_tram_line_dict = line_dict(city_tram_routes)
    
    # Get a list of all tram station names
    city_all_tram_stations_name = all_station_list(city_tram_stations)
    
    # Order stations in line for each tram line
    city_tram_order_route_dict = order_stations_inline(city_tram_line_dict, city_all_tram_stations_name, city_tram_routes, city_tram_start_station_name_dict)
    
    # Get ID pairs for ordered stations in line for each tram line
    city_tram_order_id_pairs = id_pairs_inline(city_tram_line_dict, city_tram_order_route_dict)
    
    # Create a graph for the tram network
    G = create_ground_graph(edges, nodes)
    
    # Calculate shortest paths for ordered station pairs in line for each tram line
    city_tram_shortest_path_pairs = city_tram_order_id_pairs.copy()
    duplicate_row_count = city_tram_order_id_pairs.copy()
    city_tram_shortest_path_edges = city_tram_order_id_pairs.copy()
    city_tram_edges = city_tram_order_id_pairs.copy()

    for line in city_tram_order_id_pairs.keys():
        city_tram_shortest_path_pairs[line] = all_shortest_paths(city_tram_order_id_pairs[line], edges)
        duplicate_row_count[line], city_tram_shortest_path_edges[line], city_tram_edges[line] = edges_with
    
    return plot_routes_even(city_tram_routes,edges, city_tram_shortest_path_edges), plot_routes_odd(city_tram_routes,edges, city_tram_shortest_path_edges), city_tram_shortest_path_pairs, city_tram_shortest_path_edges, city_tram_edges

def bus():




if __name__ == "__main__":

    metro(sys.argv[1])
    