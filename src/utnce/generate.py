from simplify import *
from prepare import *
from routing import *
from percolation_plot import *

import warnings
warnings.filterwarnings("ignore")

gdal.SetConfigOption("OSM_CONFIG_FILE", "osmconf.ini")


# Subway/Metro
def prepare_metro(osm_path):

    city_sub_stations = sub_stations(osm_path)

   
    city_sub_network = subway_network(osm_path)

   
    edges, nodes = prepare_network(city_sub_network, city_sub_stations)

    
    edges = expand_edges(edges)
    
   
    city_sub_stations = sub_stations(osm_path)

    
    city_sub_routes = sub_routes(osm_path)

    
    city_sub_routes = sorted_routes(city_sub_routes)

    
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


# Tram
def prepare_tram(osm_path):
    
    city_tram_stations = tram_stations(osm_path)

    city_tram_network = tram_network(osm_path)

    edges, nodes = prepare_network(city_tram_network, city_tram_stations)
    
    edges = expand_edges(edges)
    
    city_tram_routes = tram_routes(osm_path)

    return city_tram_stations, edges, nodes, city_tram_routes
    
def check_tram_routes(city_tram_stations, city_tram_routes):

    city_tram_routes = sorted_routes(city_tram_routes)

    check_name = check_to_column(city_tram_routes, city_tram_stations)

    return city_tram_routes, check_name

def recheck_tram_routes(replacement_dict, city_tram_stations, city_tram_routes):

    city_tram_routes['to'] = city_tram_routes['to'].replace(replacement_dict, regex=True).str.strip()

    city_tram_routes = city_tram_routes.reset_index(drop=True)

    check_name = check_to_column(city_tram_routes, city_tram_stations)

    return city_tram_stations, city_tram_routes, check_name

def tram(city_tram_stations, edges, nodes, city_tram_routes, city='Rotterdam'):
   
    city_tram_start_station_name_dict = start_station_dict(city_tram_routes)
    
    city_tram_line_dict = line_dict(city_tram_routes)
    

    city_all_tram_stations_name = all_station_list(city_tram_stations)
    

    city_tram_order_route_dict = order_stations_inline(city_tram_line_dict, city_all_tram_stations_name, city_tram_routes, city_tram_start_station_name_dict)
    

    city_tram_order_id_pairs = id_pairs_inline(city_tram_line_dict, city_tram_order_route_dict, nodes)
    

    G = create_ground_graph(edges, nodes)
    

    city_tram_shortest_path_pairs = city_tram_order_id_pairs.copy()
    duplicate_row_count = city_tram_order_id_pairs.copy()
    city_tram_shortest_path_edges = city_tram_order_id_pairs.copy()
    city_tram_edges = city_tram_order_id_pairs.copy()

    for line in city_tram_order_id_pairs.keys():
        
        city_tram_shortest_path_pairs[line] = all_shortest_paths(G, city_tram_order_id_pairs[line], edges)
        
        duplicate_row_count[line], city_tram_shortest_path_edges[line], city_tram_edges[line] = edges_with_count_weight(city_tram_shortest_path_pairs[line], edges)

    # plot_routes_even(city_tram_routes,edges, city_tram_shortest_path_edges)
    
    # plot_routes_odd(city_tram_routes,edges, city_tram_shortest_path_edges)
    
    plot_routes(city_tram_routes, edges, city_tram_shortest_path_edges)
    
    return city_tram_shortest_path_pairs, city_tram_shortest_path_edges, city_tram_edges


# Bus
def prepare_bus(osm_path):
    
    city_bus_stations = bus_stations(osm_path)

    city_bus_network = bus_network(osm_path)

    edges,nodes = prepare_network(city_bus_network, city_bus_stations)
    
    edges = expand_edges(edges)
    
    city_bus_routes = bus_routes(osm_path)

    return city_bus_stations, edges, nodes, city_bus_routes
    
def check_bus_routes(city_bus_stations, city_bus_routes):

    city_bus_routes = sorted_routes(city_bus_routes)

    check_name = check_to_column(city_bus_routes, city_bus_stations)

    return city_bus_routes, check_name

def recheck_bus_routes(replacement_dict, city_bus_stations, city_bus_routes):

    city_bus_routes['to'] = city_bus_routes['to'].replace(replacement_dict, regex=True).str.strip()

    city_bus_routes = city_bus_routes.reset_index(drop=True)

    check_name = check_to_column(city_bus_routes, city_bus_stations)

    return city_bus_stations, city_bus_routes, check_name

def bus(city_bus_stations, edges, nodes, city_bus_routes, city='Rotterdam'):

    city_bus_start_station_name_dict = start_station_dict(city_bus_routes)

    city_bus_line_dict = line_dict(city_bus_routes)
    
    city_bus_all_stations_name = all_station_list(city_bus_stations)
    
    city_bus_order_route_dict = order_stations_inline(city_bus_line_dict,city_bus_all_stations_name,city_bus_routes,city_bus_start_station_name_dict)
    
    city_bus_order_id_pairs = id_pairs_inline(city_bus_line_dict,city_bus_order_route_dict, nodes)
    

    G = create_ground_graph(edges, nodes)
    

    city_bus_shortest_path_pairs = city_bus_order_id_pairs.copy()
    
    city_bus_duplicate_row_count = city_bus_order_id_pairs.copy()
    
    city_bus_shortest_path_edges = city_bus_order_id_pairs.copy()
    
    city_bus_edges = city_bus_order_id_pairs.copy()

    for line in city_bus_order_id_pairs.keys():
        
        city_bus_shortest_path_pairs[line] = all_shortest_paths(G, city_bus_order_id_pairs[line],edges)
        
        city_bus_duplicate_row_count[line], city_bus_shortest_path_edges[line], city_bus_edges[line] = edges_with_count_weight(city_bus_shortest_path_pairs[line], edges)

    # plot_routes_even(city_bus_routes, edges, city_bus_shortest_path_edges)
    
    # plot_routes_odd(city_bus_routes, edges, city_bus_shortest_path_edges)
    
    plot_routes(city_bus_routes, edges, city_bus_shortest_path_edges)
    
    return city_bus_shortest_path_pairs, city_bus_shortest_path_edges, city_bus_edges
    

 