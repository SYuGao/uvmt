from simplify import *
from prepare import *
from routing import *
from percolation_plot import *

import warnings
warnings.filterwarnings("ignore")

gdal.SetConfigOption("OSM_CONFIG_FILE", "osmconf.ini")


# Subway/Metro
def prepare_metro(osm_path):
    """
    Prepares the metro network data based on the given OSM path.

    Args:
        osm_path (str): The path to the OpenStreetMap (OSM) file.

    Returns:
        tuple: A tuple containing the following metro network data:
            - city_sub_stations: Sub-stations in the city
            - edges: Edges (connections) between stations in the network
            - nodes: Nodes (stations) in the network
            - city_sub_routes: Sub-routes in the city
            - check_name: Result of the check operation
    """

    # Obtain sub-stations in the city
    city_sub_stations = sub_stations(osm_path)

    # Obtain subway network data
    city_sub_network = subway_network(osm_path)

    # Prepare the network edges and nodes based on sub-stations and subway network
    edges, nodes = prepare_network(city_sub_network, city_sub_stations)

    # Expand the edges if required
    edges = expand_edges(edges)

    # Retrieve sub-stations again (optional, as it was already obtained before)
    city_sub_stations = sub_stations(osm_path)

    # Obtain sub-routes in the city
    city_sub_routes = sub_routes(osm_path)

    # Sort the sub-routes
    city_sub_routes = sorted_routes(city_sub_routes)

    # Check the name and assign a column
    check_name = check_to_column(city_sub_routes, city_sub_stations)

    # Return all the obtained data
    return city_sub_stations, edges, nodes, city_sub_routes, check_name

def recheck_metro_routes(replacement_dict, city_sub_stations, city_sub_routes):
    """
    This function rechecks the metro routes based on the given inputs.

    Args:
    - city_sub_stations: Dataframe or data structure containing subway station information for the city.
    - replacement_dict: A dictionary used to replace certain values in the 'to' column of metro routes.
    - city_sub_routes: Dataframe or data structure containing metro route information for the city.

    Returns:
    - check_name: A list of boolean values indicating whether the 'to' column in the metro routes matches the station names.
    - city_sub_routes: a dataframe of routes after changing the start staion name according to city_sub_stations.
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
    Performs various operations on the metro network data to calculate shortest paths and plot routes.

    Args:
        city_sub_stations (list): List of sub-stations in the city.
        edges (list): List of edges (connections) between stations in the network.
        nodes (list): List of nodes (stations) in the network.
        city_sub_routes (list): List of sub-routes in the city.
        city (str, optional): Name of the city. Defaults to 'Rotterdam'.

    Returns:
        tuple: A tuple containing the following calculated data:
            - city_sub_shortest_path_pairs: Shortest path pairs for each subway line.
            - city_sub_shortest_path_edges: Shortest path edges for each subway line.
            - city_sub_edges: Subway network edges for each subway line.
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
        sub_edges = city_sub_edges[line]
    
    # # Plot the subway routes with even indices
    # plot_routes_even(city_sub_routes, edges, city_sub_shortest_path_edges)

    # # Plot the subway routes with odd indices
    # plot_routes_odd(city_sub_routes, edges, city_sub_shortest_path_edges)
        
    # Plot all subway routes
    plot_routes(city_sub_routes, edges, city_sub_shortest_path_edges)

    # Return the calculated data
    return city_sub_order_route_dict, city_sub_shortest_path_pairs, city_sub_shortest_path_edges, sub_edges #city_sub_edges


# Tram
def prepare_tram(osm_path):
    """
    Prepares the tram network data based on the given OSM path.

    Args:
        osm_path (str): The path to the OpenStreetMap (OSM) file.

    Returns:
        tuple: A tuple containing the following tram network data:
            - city_tram_stations: Tram stations in the city
            - edges: Edges (connections) between stations in the network
            - nodes: Nodes (stations) in the network
            - city_tram_routes: Tram routes in the city
    """

    # Obtain tram stations in the city
    city_tram_stations = tram_stations(osm_path)

    # Obtain tram network data
    city_tram_network = tram_network(osm_path)

    # Prepare the network edges and nodes based on tram stations and tram network
    edges, nodes = prepare_network(city_tram_network, city_tram_stations)

    # Expand the edges if required
    edges = expand_edges(edges)

    # Obtain tram routes in the city
    city_tram_routes = tram_routes(osm_path)

    # Return all the obtained data
    return city_tram_stations, edges, nodes, city_tram_routes

def check_tram_routes(city_tram_stations, city_tram_routes):
    """
    Checks the tram routes for consistency and assigns a column based on the station names.

    Args:
        city_tram_stations (list): List of tram stations in the city.
        city_tram_routes (list): List of tram routes in the city.

    Returns:
        tuple: A tuple containing the following data:
            - city_tram_routes: Sorted tram routes.
            - check_name: Result of the check operation.
    """

    # Sort the tram routes
    city_tram_routes = sorted_routes(city_tram_routes)

    # Check the name and assign a column
    check_name = check_to_column(city_tram_routes, city_tram_stations)

    # Return the sorted tram routes and check result
    return city_tram_routes, check_name

def recheck_tram_routes(replacement_dict, city_tram_stations, city_tram_routes):
    """
    Rechecks the tram routes after performing replacements based on a dictionary.

    Args:
        replacement_dict (dict): Dictionary containing replacement mappings.
        city_tram_stations (list): List of tram stations in the city.
        city_tram_routes (DataFrame): DataFrame of tram routes in the city.

    Returns:
        tuple: A tuple containing the following data:
            - city_tram_stations: List of tram stations in the city.
            - city_tram_routes: Updated DataFrame of tram routes.
            - check_name: Result of the recheck operation.
    """

    # Replace values in the 'to' column of tram routes DataFrame based on the replacement dictionary
    city_tram_routes['to'] = city_tram_routes['to'].replace(replacement_dict, regex=True).str.strip()

    # Reset the index of the tram routes DataFrame
    city_tram_routes = city_tram_routes.reset_index(drop=True)

    # Recheck the name and assign a column
    check_name = check_to_column(city_tram_routes, city_tram_stations)

    # Return the tram stations, updated tram routes DataFrame, and check result
    return city_tram_stations, city_tram_routes, check_name

def tram(city_tram_stations, edges, nodes, city_tram_routes, city='Rotterdam'):
    """
    Performs various operations on the tram network data to calculate shortest paths and plot routes.

    Args:
        city_tram_stations (list): List of tram stations in the city.
        edges (list): List of edges (connections) between stations in the network.
        nodes (list): List of nodes (stations) in the network.
        city_tram_routes (list): List of tram routes in the city.
        city (str, optional): Name of the city. Defaults to 'Rotterdam'.

    Returns:
        tuple: A tuple containing the following calculated data:
            - city_tram_shortest_path_pairs: Shortest path pairs for each tram line.
            - city_tram_shortest_path_edges: Shortest path edges for each tram line.
            - city_tram_edges: Tram network edges for each tram line.
    """

    # Create a dictionary mapping start stations to their corresponding routes
    city_tram_start_station_name_dict = start_station_dict(city_tram_routes)

    # Create a dictionary mapping tram lines to their corresponding routes
    city_tram_line_dict = line_dict(city_tram_routes)

    # Create a list of all station names in the city's tram network
    city_all_tram_stations_name = all_station_list(city_tram_stations)

    # Create a dictionary mapping tram lines to the ordered list of stations on each line
    city_tram_order_route_dict = order_stations_inline(city_tram_line_dict, city_all_tram_stations_name, city_tram_routes, city_tram_start_station_name_dict)

    # Create a dictionary mapping tram lines to the pairs of station IDs on each line
    city_tram_order_id_pairs = id_pairs_inline(city_tram_line_dict, city_tram_order_route_dict, nodes)

    # Create a graph representing the tram network using the extracted edges and nodes
    G = create_ground_graph(edges, nodes)

    # Calculate the shortest path pairs for each tram line
    city_tram_shortest_path_pairs = city_tram_order_id_pairs.copy()
    duplicate_row_count = city_tram_order_id_pairs.copy()
    city_tram_shortest_path_edges = city_tram_order_id_pairs.copy()
    city_tram_edges = city_tram_order_id_pairs.copy()

    for line in city_tram_order_id_pairs.keys():
        # Calculate all shortest paths for the current line using the tram network edges
        city_tram_shortest_path_pairs[line] = all_shortest_paths(G, city_tram_order_id_pairs[line], edges)

        # Count the duplicate rows, calculate the weighted edges for shortest paths,
        # and update the tram network edges for the current line
        duplicate_row_count[line], city_tram_shortest_path_edges[line], city_tram_edges[line] = edges_with_count_weight(city_tram_shortest_path_pairs[line], edges)
        tram_edges = city_tram_edges[line]
        
    # plot_routes_even(city_tram_routes,edges, city_tram_shortest_path_edges)
    
    # plot_routes_odd(city_tram_routes,edges, city_tram_shortest_path_edges)
        
    # Plot all tram routes
    plot_routes(city_tram_routes, edges, city_tram_shortest_path_edges)

    # Return the calculated data
    return city_tram_order_route_dict, city_tram_shortest_path_pairs, city_tram_shortest_path_edges, tram_edges # city_tram_edges

    
# Bus
def prepare_bus(osm_path):
    """
    Prepares bus-related data from OpenStreetMap (OSM) for analysis.

    Args:
        osm_path (str): The path to the OSM data.

    Returns:
        tuple: A tuple containing the following prepared data:
            - city_bus_stations (list): A list of bus stations in the city.
            - edges (list): A list of network edges for analysis.
            - nodes (list): A list of network nodes for analysis.
            - city_bus_routes (list): A list of bus routes in the city.
    """

    # Retrieve bus stations from OpenStreetMap data
    city_bus_stations = bus_stations(osm_path)

    # Retrieve bus network information from OpenStreetMap data
    city_bus_network = bus_network(osm_path)

    # Prepare the network edges and nodes for analysis
    edges, nodes = prepare_network(city_bus_network, city_bus_stations)

    # Expand the edges for more detailed information
    edges = expand_edges(edges)

    # Retrieve bus routes from OpenStreetMap data
    city_bus_routes = bus_routes(osm_path)

    # Return the prepared data
    return city_bus_stations, edges, nodes, city_bus_routes

def check_bus_routes(city_bus_stations, city_bus_routes):
    """
    Checks bus routes against bus stations and returns the sorted routes and check name.

    Args:
        city_bus_stations (list): A list of bus stations in the city.
        city_bus_routes (list): A list of bus routes in the city.

    Returns:
        tuple: A tuple containing the following checked data:
            - city_bus_routes (list): The sorted bus routes.
            - check_name (dict): A dictionary mapping routes to check names.
    """

    # Sort the bus routes
    city_bus_routes = sorted_routes(city_bus_routes)

    # Perform the check against bus stations and obtain the check name
    check_name = check_to_column(city_bus_routes, city_bus_stations)

    # Return the checked data
    return city_bus_routes, check_name

def recheck_bus_routes(replacement_dict, city_bus_stations, city_bus_routes):
    """
    Rechecks bus routes with replacement dictionary against bus stations and returns the updated data.

    Args:
        replacement_dict (dict): A dictionary mapping replacement values for 'to' column in bus routes.
        city_bus_stations (list): A list of bus stations in the city.
        city_bus_routes (DataFrame): A DataFrame containing bus routes information.

    Returns:
        tuple: A tuple containing the following updated data:
            - city_bus_stations (list): The list of bus stations in the city.
            - city_bus_routes (DataFrame): The updated DataFrame with rechecked bus routes information.
            - check_name (dict): A dictionary mapping routes to check names.
    """

    # Replace values in 'to' column of bus routes DataFrame using the replacement dictionary
    city_bus_routes['to'] = city_bus_routes['to'].replace(replacement_dict, regex=True).str.strip()

    # Reset the index of the bus routes DataFrame
    city_bus_routes = city_bus_routes.reset_index(drop=True)

    # Perform the check against bus stations and obtain the check name
    check_name = check_to_column(city_bus_routes, city_bus_stations)

    # Return the updated data
    return city_bus_stations, city_bus_routes, check_name

def bus(city_bus_stations, edges, nodes, city_bus_routes, city='Rotterdam'):
    """
    Performs bus-related operations and analysis for a specific city.

    Args:
        city_bus_stations (list): A list of bus stations in the city.
        edges (list): A list of network edges for analysis.
        nodes (list): A list of network nodes for analysis.
        city_bus_routes (list): A list of bus routes in the city.
        city (str, optional): The name of the city. Defaults to 'Rotterdam'.

    Returns:
        tuple: A tuple containing the following analyzed data:
            - city_bus_shortest_path_pairs (dict): A dictionary mapping bus lines to their shortest path pairs.
            - city_bus_shortest_path_edges (dict): A dictionary mapping bus lines to their shortest path edges.
            - city_bus_edges (dict): A dictionary mapping bus lines to their all edges.
    """

    # Create a dictionary mapping bus routes to their starting station names
    city_bus_start_station_name_dict = start_station_dict(city_bus_routes)

    # Create a dictionary mapping bus lines to their line information
    city_bus_line_dict = line_dict(city_bus_routes)
    
    # Create a list of all station names
    city_bus_all_stations_name = all_station_list(city_bus_stations)
    
    # Create a dictionary mapping bus lines to the ordered route of stations
    city_bus_order_route_dict = order_stations_inline(city_bus_line_dict, city_bus_all_stations_name, city_bus_routes, city_bus_start_station_name_dict)
    
    # Create a dictionary mapping bus lines to their ordered ID pairs
    city_bus_order_id_pairs = id_pairs_inline(city_bus_line_dict, city_bus_order_route_dict, nodes)

    # Create the ground graph
    G = create_ground_graph(edges, nodes)

    # Create copies of dictionaries to store the analyzed data
    city_bus_shortest_path_pairs = city_bus_order_id_pairs.copy()
    city_bus_duplicate_row_count = city_bus_order_id_pairs.copy()
    city_bus_shortest_path_edges = city_bus_order_id_pairs.copy()
    city_bus_edges = city_bus_order_id_pairs.copy()

    # Iterate over each line in the bus order ID pairs
    for line in city_bus_order_id_pairs.keys():
        # Compute all shortest paths for the line
        city_bus_shortest_path_pairs[line] = all_shortest_paths(G, city_bus_order_id_pairs[line], edges)
        
        # Compute edges with count and weight information for the line
        city_bus_duplicate_row_count[line], city_bus_shortest_path_edges[line], city_bus_edges[line] = edges_with_count_weight(city_bus_shortest_path_pairs[line], edges)
        bus_edges = city_bus_edges[line]

    # # Plot the routes with even IDs
    # plot_routes_even(city_bus_routes, edges, city_bus_shortest_path_edges)
    
    # # Plot the routes with odd IDs
    # plot_routes_odd(city_bus_routes, edges, city_bus_shortest_path_edges)
    
    # Plot all the routes
    plot_routes(city_bus_routes, edges, city_bus_shortest_path_edges)
    
    # Return the analyzed data
    return city_bus_order_route_dict, city_bus_shortest_path_pairs, city_bus_shortest_path_edges, bus_edges # city_bus_edges



# Plot only one route
def plot_line(edges, shortest_path_edges_dict, key):
    """
    Plot a specific line from a dictionary of shortest path edges.

    Args:
        edges (DataFrame or GeoDataFrame): All edges to be plotted as background.
        shortest_path_edges_dict (dict): Dictionary of shortest path edges.
        key (str): The specific key representing the desired line in the dictionary.

    Returns:
        None
    """

    # Create a figure and axis for the plot
    fig, ax = plt.subplots(1, 1, figsize=(30, 20))

    # Plot all edges as the background in gray with reduced opacity
    gpd.GeoDataFrame(edges.copy()).plot(ax=ax, color='gray', alpha=0.2)

    # Retrieve the specific line based on the provided key
    specific_line = shortest_path_edges_dict[key]

    # Plot the specific line on top with increased linewidth and orange color
    gpd.GeoDataFrame(specific_line.copy()).plot(ax=ax, zorder=1, linewidth=(specific_line.count_weight) * 2, color='orange')

    # Set the title of the plot as the provided key
    ax.set_title(key)