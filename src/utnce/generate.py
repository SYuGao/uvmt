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


def prepare_metro(osm_path):
    """
    This function checks the metro routes based on the given inputs.

    Inputs:
    - osm_path: The path to the OpenStreetMap (OSM) file containing subway network information.
    - city: The name of the city. Default is 'Rotterdam'.

    Outputs:
    - check_name: A list of boolean values indicating whether the 'to' column in the metro routes matches the station names.
    """
    # Retrieve sub-stations information for the specified city
    city_sub_stations = sub_stations(osm_path)

    # Retrieve subway network information for the specified city
    city_sub_network = subway_network(osm_path)

    # Prepare the network by extracting edges and nodes relevant to the specified city
    edges, nodes = prepare_network(city_sub_network, city_sub_stations)

    # Expand edges to include additional information if necessary
    edges = expand_edges(edges)
    
    # Retrieve sub-stations information for the specified city
    city_sub_stations = sub_stations(osm_path)

    # Retrieve metro routes information for the specified city
    city_sub_routes = sub_routes(osm_path)

    # Sort metro routes data
    city_sub_routes = sorted_routes(city_sub_routes)

    # Check if the values in the 'to' column of the metro routes match the station names
    check_name = check_to_column(city_sub_routes, city_sub_stations)

    return city_sub_stations, edges, nodes, city_sub_routes, check_name

def recheck_metro_routes(replacement_dict, city_sub_stations, city_sub_routes):
    """
    This function rechecks the metro routes based on the given inputs.

    Inputs:
    - city_sub_stations: Dataframe or data structure containing subway station information for the city.
    - replacement_dict: A dictionary used to replace certain values in the 'to' column of metro routes.
    - city_sub_routes: Dataframe or data structure containing metro route information for the city.

    Outputs:
    - check_name: A list of boolean values indicating whether the 'to' column in the metro routes matches the station names.
    """

    # Replace values in the 'to' column of the metro routes using the replacement dictionary
    city_sub_routes['to'] = city_sub_routes['to'].replace(replacement_dict, regex=True).str.strip()

    # Reset the index of the metro routes
    city_sub_routes = city_sub_routes.reset_index(drop=True)

    # Check if the values in the 'to' column of the metro routes match the station names
    check_name = check_to_column(city_sub_routes, city_sub_stations)

    return city_sub_routes, check_name

def metro(city_sub_stations, edges, nodes, city_sub_routes, city='Rotterdam'):
    """
    This function generates subway route information based on the given inputs.

    Inputs:
    - osm_path: The path to the OpenStreetMap (OSM) file containing subway network information.
    - city_sub_routes: Dataframe or data structure containing subway route information for the specified city.
    - city: The name of the city. Default is 'Rotterdam'.

    Outputs:
    - city_sub_shortest_path_pairs: A dictionary mapping subway lines to a list of shortest path pairs.
    - city_sub_shortest_path_edges: A dictionary mapping subway lines to a list of edges in the shortest paths.
    - city_sub_edges: A dictionary mapping subway lines to a list of all edges in the subway network.
    """

    # Create a dictionary mapping start stations to their corresponding routes
    city_sub_start_station_name_dict = start_station_dict(city_sub_routes)

    # Create a dictionary mapping subway lines to their corresponding routes
    city_sub_line_dict = line_dict(city_sub_routes)

    # Create a list of all station names in the city's subway network
    city_all_sub_stations_name = all_station_list(city_sub_stations)

    # Create a dictionary mapping subway lines to the ordered list of stations on each line
    city_sub_order_route_dict = order_stations_inline(city_sub_line_dict, city_all_sub_stations_name, city_sub_routes, city_sub_start_station_name_dict)

    # Create a dictionary mapping subway lines to the pairs of station IDs on each line
    city_sub_order_id_pairs = id_pairs_inline(city_sub_line_dict, city_sub_order_route_dict, nodes)

    # Create a graph representing the subway network using the extracted edges and nodes
    G = create_ground_graph(edges, nodes)

    # Calculate the shortest path pairs for each subway line
    city_sub_shortest_path_pairs = city_sub_order_id_pairs.copy()
    duplicate_row_count = city_sub_order_id_pairs.copy()
    city_sub_shortest_path_edges = city_sub_order_id_pairs.copy()
    city_sub_edges = city_sub_order_id_pairs.copy()

    for line in city_sub_order_id_pairs.keys():
        # Calculate all shortest paths for the current line using the subway network edges
        city_sub_shortest_path_pairs[line] = all_shortest_paths(G, city_sub_order_id_pairs[line], edges)

        # Count the duplicate rows, calculate the weighted edges for shortest paths,
        # and update the subway network edges for the current line
        duplicate_row_count[line], city_sub_shortest_path_edges[line], city_sub_edges[line] = edges_with_count_weight(city_sub_shortest_path_pairs[line], edges)

    # # Plot the subway routes with even indices
    # plot_routes_even(city_sub_routes, edges, city_sub_shortest_path_edges)

    # # Plot the subway routes with odd indices
    # plot_routes_odd(city_sub_routes, edges, city_sub_shortest_path_edges)

    # Plot all subway routes
    plot_routes(city_sub_routes, edges, city_sub_shortest_path_edges)

    return city_sub_shortest_path_pairs, city_sub_shortest_path_edges, city_sub_edges




def prepare_tram(osm_path):
    
    city_tram_stations = tram_stations(osm_path)

    city_tram_routes = tram_routes(osm_path)

    return city_tram_stations, city_tram_routes
    
def check_tram_routes(city_tram_stations, city_tram_routes):

    city_tram_routes = sorted_routes(city_tram_routes)

    check_name = check_to_column(city_sub_routes, city_sub_stations)

    return city_tram_routes, check_name

def recheck_tram_routes(replacement_dict, city_tram_stations, city_tram_routes):

    city_tram_routes['to'] = city_tram_routes['to'].replace(replacement_dict, regex=True).str.strip()

    city_tram_routes = city_tram_routes.reset_index(drop=True)

    check_name = check_to_column(city_tram_routes, city_tram_stations)

    return city_tram_routes, city_tram_stations, check_name

def tram(osm_path, city_tram_routes, city='Rotterdam'):
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
        duplicate_row_count[line], city_tram_shortest_path_edges[line], city_tram_edges[line] = edges_with_count_weight(city_tram_shortest_path_pairs[line],edges)

    plot_routes_even(city_tram_routes,edges, city_tram_shortest_path_edges)
    
    plot_routes_odd(city_tram_routes,edges, city_tram_shortest_path_edges)
    
    return city_tram_shortest_path_pairs, city_tram_shortest_path_edges, city_tram_edges









    


# def tram(osm_path,city='Rotterdam'):
#     """
#     Performs various operations related to trams.
    
#     Parameters:
#         osm_path (str): The path to the OpenStreetMap data.
#         city (str) : The name of the city.
    
#     Returns:
#         tuple: A tuple containing multiple outputs related to trams.
#     """
#     # Retrieve tram stations from the OSM file
#     city_tram_stations = tram_stations(osm_path)
    
#     # Retrieve tram network information from the OSM file
#     city_tram_network = tram_network(osm_path)
    
#     # Prepare network edges and nodes based on tram stations and network information
#     edges, nodes = prepare_network(city_tram_network, city_tram_stations)
    
#     # Expand edges (if required)
#     edges = expand_edges(edges)
    
#     # Retrieve tram routes information from the OSM file
#     city_tram_routes = tram_routes(osm_path)
    
#     # Exclude tram routes with 'EMA' as the reference
#     city_tram_routes = city_tram_routes.loc[city_tram_routes.ref != 'EMA']
    
#     # Sort tram routes
#     city_tram_routes = sorted_routes(city_tram_routes)
    
#     # Check and revise tram routes and stations
#     need_revised_row = check_to_column(city_tram_routes, city_tram_stations)
    
#     # Define replacement dictionary for specific string replacements
#     replacement_dict = {
#         ',': '',
#         'Diemen': '',
#         'Amsterdam': '',
#         'Sloterdijk': 'Station Sloterdijk',
#         'Osdorp Dijkgraafsplein': 'Dijkgraafplein',
#         'Osdorp De Aker': 'Matterhorn'
#     }
    
#     # Perform string replacements in the 'to' column of tram routes
#     city_tram_routes['to'] = city_tram_routes['to'].replace(replacement_dict, regex=True).str.strip()
#     city_tram_routes['to'] = city_tram_routes['to'].replace('Amstelveen Westwijk', 'Westwijk').str.strip()
#     city_tram_routes = city_tram_routes.reset_index(drop=True)
    
#     # Create dictionaries for start stations and tram lines
#     city_tram_start_station_name_dict = start_station_dict(city_tram_routes)
#     city_tram_line_dict = line_dict(city_tram_routes)
    
#     # Get a list of all tram station names
#     city_all_tram_stations_name = all_station_list(city_tram_stations)
    
#     # Order stations in line for each tram line
#     city_tram_order_route_dict = order_stations_inline(city_tram_line_dict, city_all_tram_stations_name, city_tram_routes, city_tram_start_station_name_dict)
    
#     # Get ID pairs for ordered stations in line for each tram line
#     city_tram_order_id_pairs = id_pairs_inline(city_tram_line_dict, city_tram_order_route_dict)
    
#     # Create a graph for the tram network
#     G = create_ground_graph(edges, nodes)
    
#     # Calculate shortest paths for ordered station pairs in line for each tram line
#     city_tram_shortest_path_pairs = city_tram_order_id_pairs.copy()
#     duplicate_row_count = city_tram_order_id_pairs.copy()
#     city_tram_shortest_path_edges = city_tram_order_id_pairs.copy()
#     city_tram_edges = city_tram_order_id_pairs.copy()

#     for line in city_tram_order_id_pairs.keys():
#         city_tram_shortest_path_pairs[line] = all_shortest_paths(city_tram_order_id_pairs[line], edges)
#         duplicate_row_count[line], city_tram_shortest_path_edges[line], city_tram_edges[line] = edges_with_count_weight(city_tram_shortest_path_pairs[line],edges)
    
#     return plot_routes_even(city_tram_routes,edges, city_tram_shortest_path_edges), plot_routes_odd(city_tram_routes,edges, city_tram_shortest_path_edges), city_tram_shortest_path_pairs, city_tram_shortest_path_edges, city_tram_edges    