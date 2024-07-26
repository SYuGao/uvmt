import geopandas
import geopandas as gpd
import pandas as pd
from osgeo import ogr,gdal
import numpy 
from shapely.wkb import loads
from shapely.geometry import Point
from shapely.geometry import LineString
from geopy.distance import geodesic
from pyproj import Proj, Transformer
import matplotlib.pyplot as plt
import openpyxl
import itertools
import networkx as nx
from simplify import *
from prepare import *
from routing import *
from generate import *

# Add 'ref' and 'route' information from routes_df to order_route_dict
def add_ref_to_orderroutes_or_shortestpath_dict(order_route_dict, routes_df):
    """
    Add 'ref' and 'route' information from routes_df to order_route_dict.

    Args:
        order_route_dict (dict): A dictionary where keys are route names and values are DataFrames containing route data.
        routes_df (pandas.DataFrame): A DataFrame containing information about routes, including 'name', 'ref', and 'route' columns.

    Returns:
        dict: The modified order_route_dict with 'ref' and 'route' information added to each route's DataFrame.
    """
    
    # Get a list of route names from the keys of order_route_dict
    route_name_list = list(order_route_dict.keys())
    
    # Assign the route name to a new column 'route_name_list' in each DataFrame in order_route_dict
    for key, df in order_route_dict.items():
        df['route_name_list'] = key

    # Iterate through each route in order_route_dict
    for line in order_route_dict.keys():
        order_route_list = order_route_dict[line]
        
        # Iterate through each row in the route's DataFrame
        for i in range(len(order_route_list)):
            
            # Iterate through each row in routes_df to find a match based on 'name'
            for j in range(len(routes_df)):
                
                # Check if the 'name' in order_route_list matches the 'name' in routes_df
                if order_route_list.iloc[i]['route_name_list'] == routes_df.iloc[j]['name']:
                    
                    # If there is a match, assign 'ref' and 'route' values to the corresponding row in order_route_list
                    order_route_list.at[i, 'ref'] = routes_df.iloc[j]['ref']
                    order_route_list.at[i, 'route'] = routes_df.iloc[j]['route']
                    
                    # Break the inner loop as we found a match for the current route
                    break 
        
        # Update the route in order_route_dict with the modified order_route_list
        order_route_dict[line] = order_route_list
        
    # Return the modified order_route_dict
    return order_route_dict

# Enriches the 'nodes' DataFrame with additional columns based on data from 'order_route_dict'
# First, create a new column 'coordinate_value' in the DataFrame df by combining 'geo_x' and 'geo_y' columns into tuples
def create_tuple_column(df):
    """
    Create a new column 'coordinate_value' in the DataFrame df by combining 'geo_x' and 'geo_y' columns into tuples.

    Args:
        df (pandas.DataFrame): The input DataFrame containing 'geo_x' and 'geo_y' columns.

    Returns:
        pandas.DataFrame: The modified DataFrame with an additional 'coordinate_value' column.

    Example:
        Input DataFrame:
        |  geo_x  |  geo_y  |
        |---------|---------|
        |  1.234  |  3.456  |
        |  2.345  |  4.567  |

        Output DataFrame:
        |  geo_x  |  geo_y  |  coordinate_value  |
        |---------|---------|--------------------|
        |  1.234  |  3.456  |   (1.234, 3.456)   |
        |  2.345  |  4.567  |   (2.345, 4.567)   |
    """
    
    # Combine 'geo_x' and 'geo_y' columns into tuples and store the result in a new 'coordinate_value' column
    df['coordinate_value'] = list(zip(df['geo_x'], df['geo_y']))
    
    # Return the modified DataFrame
    return df

def add_columns_to_nodes(order_route_dict, nodes):
    """
    Enriches the 'nodes' DataFrame with additional columns based on data from 'order_route_dict'.

    Args:
        order_route_dict (dict): A dictionary where keys are route names and values are DataFrames containing route data.
        nodes (geopandas.GeoDataFrame): A GeoDataFrame containing information about nodes/points.

    Returns:
        geopandas.GeoDataFrame: The modified 'nodes' GeoDataFrame with additional columns added.
    """
    
    # Iterate through each route in order_route_dict
    for key, df in order_route_dict.items():
        # Create a new DataFrame 'new_df' by calling 'create_tuple_column' function on the route's DataFrame
        new_df = create_tuple_column(df)
        # Update the route's DataFrame in order_route_dict with the newly created 'new_df'
        order_route_dict[key] = new_df

    # Concatenate all DataFrames in order_route_dict and reset the index
    order_route_stations_df = pd.concat(order_route_dict.values()).reset_index(drop=True)

    # Create a DataFrame 'stations_name_ref' containing only 'name' and 'ref' columns and remove duplicates
    stations_name_ref = order_route_stations_df[['name', 'ref']].drop_duplicates().reset_index(drop=True)
    
    # Group 'stations_name_ref' by 'name' and aggregate 'ref' values into a comma-separated string
    stations_name_ref = stations_name_ref.groupby('name').agg({'ref': lambda x: ', '.join(x)}).reset_index()

    # Merge 'order_route_stations_df' with 'stations_name_ref' on 'name' to add 'ref' information
    order_route_stations_df = pd.merge(order_route_stations_df, stations_name_ref, on='name', how='left')
    
    # Remove duplicates based on 'coordinate_value' and select relevant columns
    order_route_stations_df = order_route_stations_df.drop_duplicates(subset='coordinate_value').reset_index(drop=True)
    order_route_stations_df = order_route_stations_df[['name', 'geometry', 'geo_x', 'geo_y', 'coordinate_value', 'route_name_list', 'route', 'ref_y']]
    
    # Rename the 'ref_y' column to 'ref'
    order_route_stations_df.rename(columns={'ref_y': 'ref'}, inplace=True)

    # Merge 'nodes' with 'order_route_stations_df' based on 'geometry' to add additional data to 'nodes'
    new_nodes = pd.merge(nodes, order_route_stations_df, on='geometry', how='right')
    
    # Create a GeoDataFrame 'new_nodes_gdf' from 'new_nodes' and extract 'geo_x', 'geo_y', and 'coordinate_value' columns
    new_nodes_gdf = gpd.GeoDataFrame(new_nodes.copy())
    new_nodes_gdf['geo_x'] = new_nodes_gdf.geometry.x
    new_nodes_gdf['geo_y'] = new_nodes_gdf.geometry.y
    new_nodes_gdf['coordinate_value'] = list(zip(new_nodes_gdf['geo_x'], new_nodes_gdf['geo_y']))
    new_nodes = new_nodes_gdf

    # Initialize a 'transfer' column in 'new_nodes' with None values
    new_nodes['transfer'] = None
    
    # Define a function 'fill_transfer' to populate 'transfer' based on 'ref'
    def fill_transfer(row):
        if ',' in row['ref']:
            return row['ref']
        else:
            return None
    
    # Apply the 'fill_transfer' function to populate 'transfer' column
    new_nodes['transfer'] = new_nodes.apply(fill_transfer, axis=1)

    # # create a exactly same dataframe with new_nodes but name columns by different transportation routes
    # rename_cols = {
    # "geometry": "geometry"+"_"+new_nodes.loc[0,'route'],
    # "degree": "degree"+"_"+new_nodes.loc[0,'route'],
    # "id": "id"+"_"+new_nodes.loc[0,'route'],
    # "name": "name"+"_"+new_nodes.loc[0,'route'],
    # "geo_x": "geo_x"+"_"+new_nodes.loc[0,'route'],
    # "geo_y": "geo_y"+"_"+new_nodes.loc[0,'route'],
    # "coordinate_value": "coordinate_value"+"_"+new_nodes.loc[0,'route'],
    # "route_name_list": "route_name_list"+"_"+new_nodes.loc[0,'route'],
    # "route": "route"+"_"+new_nodes.loc[0,'route'],
    # "ref": "ref"+"_"+new_nodes.loc[0,'route'],
    # "transfer": "transfer"+"_"+new_nodes.loc[0,'route']
    # }
    # new_nodes_ftn = new_nodes.rename(columns=rename_cols)
    
    # Return the modified 'new_nodes','new_nodes_ftn' GeoDataFrame
    return new_nodes

def all_transfer_stations_df(new_nodes):
    """
    Extracts transfer stations from a DataFrame of nodes based on the 'ref' column.

    Parameters:
    - new_nodes (pandas.DataFrame): Input DataFrame containing nodes information.

    Returns:
    pandas.DataFrame: DataFrame containing transfer stations based on the 'ref' column.

    Example:
    >>> import pandas as pd
    >>> data = {'id': [1, 2, 3], 'name': ['Station A', 'Station B', 'Station C'], 'ref': ['A,B', 'C', 'D,E']}
    >>> nodes_df = pd.DataFrame(data)
    >>> all_transfer_stations_df(nodes_df)
       id       name  ref
    0   1  Station A  A,B
    2   3  Station C  D,E
    """
    # Filter rows where 'ref' column contains a comma (',') indicating transfer stations
    transfer_stations_df = new_nodes[new_nodes['ref'].str.contains(',')]

    return transfer_stations_df
    
# Enriches the 'edges' DataFrame with additional columns based on data from 'shortest_path_edges'
def add_columns_to_edges(shortest_path_edges, edges):
    """
    Enriches the 'edges' DataFrame with additional columns based on data from 'shortest_path_edges'.

    Args:
        shortest_path_edges (dict): A dictionary where keys are edge IDs and values are DataFrames containing edge data.
        edges (pandas.DataFrame): A DataFrame containing information about edges, including 'geometry', 'osm_id', and other columns.

    Returns:
        pandas.DataFrame: The modified 'edges' DataFrame with additional columns added.
    """
    
    # Concatenate all DataFrames in shortest_path_edges into shortest_path_edges_df and reset the index
    shortest_path_edges_df = pd.concat(shortest_path_edges.values()).reset_index(drop=True)

    # Create a DataFrame 'edges_id_ref' containing only 'id' and 'ref' columns and remove duplicates
    edges_id_ref = shortest_path_edges_df[['id', 'ref']].drop_duplicates().reset_index(drop=True)
    
    # Group 'edges_id_ref' by 'id' and aggregate 'ref' values into a comma-separated string
    edges_id_ref = edges_id_ref.groupby('id').agg({'ref': lambda x: ', '.join(x)}).reset_index()

    # Merge 'shortest_path_edges_df' with 'edges_id_ref' on 'id' to add 'ref' information
    shortest_path_edges_df = pd.merge(shortest_path_edges_df, edges_id_ref, on='id', how='left')
    
    # Remove duplicate rows based on 'id', drop the 'ref_x' column, and rename 'ref_y' to 'ref'
    shortest_path_edges_df = shortest_path_edges_df.drop_duplicates(subset='id').reset_index(drop=True)
    shortest_path_edges_df = shortest_path_edges_df.drop(columns=['ref_x'])
    shortest_path_edges_df = shortest_path_edges_df.rename(columns={'ref_y': 'ref'})

    # convert LineString into WKT（well_known Text）
    edges_df = pd.DataFrame(edges.copy())
    edges_df['geometry_str'] = edges_df['geometry'].apply(lambda geom: geom.wkt if geom else None)
    shortest_path_edges_df['geometry_str'] = shortest_path_edges_df['geometry'].apply(lambda geom: geom.wkt if geom else None)

    # Merge 'edges' with 'shortest_path_edges_df' based on 'geometry' to add additional data to 'edges'
    new_edges = pd.merge(edges_df, shortest_path_edges_df, on='geometry_str', how='outer')
    # new_edges = pd.merge(edges, shortest_path_edges_df, on='geometry', how='outer')
    
    # Select and rename relevant columns in 'new_edges'
    new_edges = new_edges[['osm_id_x', 'geometry_x', 'railway_x', 'service_x', 'id_x', 'from_id_x', 'to_id_x', 'distance_x', 'time_x', 'to_from_x', 'from_to_x', 'count_weight', 'route_name_list', 'route', 'ref']]
    new_edges = new_edges.rename(columns={'osm_id_x': 'osm_id',
                                         'geometry_x':'geometry',
                                         'railway_x': 'railway',
                                         'service_x': 'service',
                                         'id_x': 'id',
                                         'from_id_x': 'from_id',
                                         'to_id_x': 'to_id',
                                         'distance_x': 'distance',
                                         'time_x': 'time',
                                         'to_from_x': 'to_from',
                                         'from_to_x': 'from_to'})
    
    # Return the modified 'new_edges' DataFrame
    return new_edges


# Get start_node and end_node stations using coordinates of start and end points
def s_e_node_df(s_e_coordinates, new_nodes):
    """
    Get start_node and end_node stations in a DataFrame using coordinates of start and end points.

    Parameters:
    - s_e_coordinates (dict): Dictionary containing 'start' and 'end' coordinates.
    - new_nodes (pandas.DataFrame): Input DataFrame containing nodes information.

    Returns:
    Tuple[pandas.DataFrame, pandas.DataFrame]: A tuple containing DataFrames for the start and end nodes.

    Example:
    >>> import pandas as pd
    >>> coordinates = {'start': {'lat': 40.123, 'lon': -74.567}, 'end': {'lat': 41.789, 'lon': -73.456}}
    >>> data = {'id': [1, 2, 3], 'name': ['Node A', 'Node B', 'Node C'], 'lat': [40.0, 41.0, 42.0], 'lon': [-75.0, -74.5, -73.0]}
    >>> nodes_df = pd.DataFrame(data)
    >>> start_node, end_node = s_e_node_df(coordinates, nodes_df)
    >>> start_node
       id     name   lat   lon
    0   1  Node A  40.0 -75.0
    >>> end_node
       id     name   lat   lon
    2   3  Node C  42.0 -73.0
    """
    # Create DataFrame from start and end coordinates
    start_end_points_coordinates_pairs = pd.DataFrame([s_e_coordinates])
    start_end_points_coordinates_pairs = s_e_coordinates_pairs(start_end_points_coordinates_pairs)

    # Find nearest node IDs for start and end points
    start_end_nearest_id_pairs = id_pairs(start_end_points_coordinates_pairs, new_nodes)

    # Retrieve start and end nodes based on their IDs
    start_node = new_nodes[new_nodes['id'] == start_end_nearest_id_pairs.loc[0]['s_id']]
    end_node = new_nodes[new_nodes['id'] == start_end_nearest_id_pairs.loc[0]['e_id']]

    return start_node, end_node
    
### Determine whether the start_node and end_node are on the same routes
# firstly,extracts route references from start_node, end_node, and returns them as DataFrames
def s_e_on_route_ref(start_node, end_node):
    """
    Extracts route references from start and end nodes, and returns them as DataFrames.

    Parameters:
    - start_node (pandas.DataFrame): DataFrame representing the start node with a 'ref' column.
    - end_node (pandas.DataFrame): DataFrame representing the end node with a 'ref' column.

    Returns:
    Tuple[pandas.DataFrame, pandas.DataFrame]: Two DataFrames containing route references.

    Example:
    >>> import pandas as pd
    >>> start_data = {'id': [1], 'name': ['Start Station'], 'ref': ['A, B, C']}
    >>> end_data = {'id': [2], 'name': ['End Station'], 'ref': ['C, D, E']}
    >>> start_node_df = pd.DataFrame(start_data)
    >>> end_node_df = pd.DataFrame(end_data)
    >>> s_e_on_route_ref(start_node_df, end_node_df)
      ref
    0   A
    1   B
    2   C,  ref
    0   C
    1   D
    2   E
    """
    # Extract route references from the 'ref' column of start_node
    start_node_ref = start_node.ref.to_list()
    s_ref_elements = start_node_ref[0].split(', ')
    s_on_route_ref = pd.DataFrame({'ref': s_ref_elements})

    # Extract route references from the 'ref' column of end_node
    end_node_ref = end_node.ref.to_list()
    e_ref_elements = end_node_ref[0].split(', ')
    e_on_route_ref = pd.DataFrame({'ref': e_ref_elements})

    return s_on_route_ref, e_on_route_ref

# secondly,determine the relationship between two sets of routes represented by 's_on_route_ref' and 'e_on_route_ref'
def judge_on_route(s_on_route_ref, e_on_route_ref):
    """
    Determine the relationship between two sets of routes represented by 's_on_route_ref' and 'e_on_route_ref'.

    Parameters:
    - s_on_route_ref (pandas.DataFrame): DataFrame representing routes related to the starting point.
    - e_on_route_ref (pandas.DataFrame): DataFrame representing routes related to the ending point.

    Returns:
    Tuple of pandas.DataFrames: A tuple containing 's_on_route_ref' and 'e_on_route_ref' after analysis.

    Example:
    >>> s_routes = pd.DataFrame({'id': [1, 2], 'name': ['Route A', 'Route B'], 'ref': ['A', 'B']})
    >>> e_routes = pd.DataFrame({'id': [3, 4], 'name': ['Route B', 'Route C'], 'ref': ['B', 'C']})
    >>> judge_on_route(s_routes, e_routes)
    s_node and e_node are on two different routes
    The next step is to find transfer stations of e_on_route and s_on_route----using function 'transfer_station'
    (   id    name ref
    0   1  Route A   A
    1   2  Route B   B,
       id    name ref
    0   3  Route B   B
    1   4  Route C   C)
    """

    # Check the length of s_on_route_ref
    length_s_on_route_ref = len(s_on_route_ref)
    
    # Check the length of e_on_route_ref
    length_e_on_route_ref = len(e_on_route_ref)

    # If s_on_route_ref is empty, print an error message
    if length_s_on_route_ref < 1:
        print("There is something wrong with the function - s_on_route_ref is empty.")
    
    # If there is only one route in s_on_route_ref
    elif length_s_on_route_ref == 1:
        # If there is more than one route in e_on_route_ref
        if length_e_on_route_ref > 1:
            # Concatenate s_on_route_ref and e_on_route_ref
            s_e_on_route = pd.concat([s_on_route_ref, e_on_route_ref], ignore_index=True)
            
            # If there is exactly one duplicated route (same route in both s_on_route_ref and e_on_route_ref)
            if s_e_on_route.duplicated().sum() == 1:
                print("One of e_on_route is the same as s_on_route.")
                print("The next step is to find the same route ---- using function 's_e_same_routes'")
                return s_on_route_ref, e_on_route_ref
            else:
                print("None of e_on_route is the same as s_on_route.")
                print("The next step is to find transfer stations of e_on_route and s_on_route ---- using function 'transfer_station'")
                return s_on_route_ref, e_on_route_ref

        # If there is only one route in e_on_route_ref
        elif length_e_on_route_ref == 1:
            # Check if the single route in s_on_route_ref is the same as the single route in e_on_route_ref
            if s_on_route_ref.equals(e_on_route_ref):
                print("s_node and e_node are on one same route.")
                print("The next step is to find the same route ---- using function 's_e_same_routes'")
                return s_on_route_ref, e_on_route_ref
            else:
                print("s_node and e_node are on two different routes.")
                print("The next step is to find transfer stations of e_on_route and s_on_route ---- using function 'transfer_station'")
                return s_on_route_ref, e_on_route_ref

        else:
            print("There is something wrong with the function - s_e_on_route_gdf")

    # If there is more than one route in s_on_route_ref
    else:
        # Concatenate s_on_route_ref and e_on_route_ref
        s_e_on_route = pd.concat([s_on_route_ref, e_on_route_ref], ignore_index=True)

        # If there is more than one route in e_on_route_ref
        if length_e_on_route_ref > 1:
            # If all routes in e_on_route_ref are the same as any route in s_on_route_ref
            if s_e_on_route.duplicated().sum() == len(s_on_route_ref):
                print("s_node and e_node are on several same routes.")
                print("The next step is to find the same routes ---- using function 's_e_same_routes'")
                return s_on_route_ref, e_on_route_ref
            
            # If none of the routes in e_on_route_ref are the same as any route in s_on_route_ref
            elif s_e_on_route.duplicated().sum() == 0:
                print("Any of e_on_route is not the same as any of s_on_route.")
                print("The next step is to find transfer stations of e_on_route and s_on_route ---- using function 'transfer_station'")
                return s_on_route_ref, e_on_route_ref
            
            # If some routes in e_on_route_ref are the same as some routes in s_on_route_ref
            else:
                # Extract routes that are duplicated (same routes in both s_on_route_ref and e_on_route_ref)
                s_e_same_route = s_e_on_route[s_e_on_route['ref'].duplicated()]
                print("Some of e_on_route is the same as some of s_on_route.")
                print("The next step is to find the same routes ---- using function 's_e_same_routes'")
                return s_on_route_ref, e_on_route_ref

        # If there is only one route in e_on_route_ref
        elif length_e_on_route_ref == 1:
            # If there is exactly one duplicated route (same route in both s_on_route_ref and e_on_route_ref)
            if s_e_on_route.duplicated().sum() == 1:
                print("One of s_on_route is the same as e_on_route.")
                print("The next step is to find the same routes ---- using function 's_e_same_routes'")
                return s_on_route_ref, e_on_route_ref
            
            # If none of the routes in s_on_route_ref are the same as the single route in e_on_route_ref
            else:
                print("None of s_on_route is the same as e_on_route.")
                print("The next step is to find transfer stations of e_on_route and s_on_route ---- using function 'transfer_station'")
                return s_on_route_ref, e_on_route_ref

        else:
            print("There is something wrong with the function - s_e_on_route_gdf")

            
# If the start_node and end_node are not on any same route, the next step is to find transfer stations of e_on_route and s_on_route----using function 'transfer_station_one_mode'
def transfer_station_one_mode(routes_df, start_node, end_node, new_nodes, order_route_dict):
    """
    Identify a transfer station for a one-mode transportation network between the given start and end nodes.

    Parameters:
    - routes_df (pandas.DataFrame): DataFrame containing information about routes, including geometry.
    - start_node (pandas.DataFrame): DataFrame containing information about the starting node.
    - end_node (pandas.DataFrame): DataFrame containing information about the ending node.
    - new_nodes (geopandas.GeoDataFrame): GeoDataFrame containing information about all nodes.
    - order_route_dict (dict): Dictionary containing order information about routes.

    Returns:
    geopandas.GeoDataFrame: GeoDataFrame containing the identified transfer station.

    Example:
    >>> transfer_station_one_mode(routes_df, start_node, end_node, new_nodes, order_route_dict)
    GeoDataFrame with information about the identified transfer station.
    """

    # Create GeoDataFrames for routes, starting node, and ending node
    routes_gdf = gpd.GeoDataFrame(routes_df.copy())
    s_node_gdf = gpd.GeoDataFrame(start_node.copy())
    e_node_gdf = gpd.GeoDataFrame(end_node.copy())

    # Filter routes that intersect with the geometry of the starting node
    s_node_on_route_gdf = routes_gdf[routes_gdf.geometry.intersects(s_node_gdf.iloc[0].geometry)]

    # Filter routes that intersect with the geometry of the ending node
    e_node_on_route_gdf = routes_gdf[routes_gdf.geometry.intersects(e_node_gdf.iloc[0].geometry)]

    # Create a set of route references from the starting and ending nodes
    t_station_ref_set = set([s_node_on_route_gdf.iloc[0].ref, e_node_on_route_gdf.iloc[0].ref])

    # Extract all nodes with references that contain the set of route references
    all_nodes_ref = new_nodes.ref.to_list()
    all_intersects_results = [set(item.split(', ')).issuperset(t_station_ref_set) for item in all_nodes_ref]
    t_stations_gdf = new_nodes.iloc[all_intersects_results]
    t_stations_n_c = t_stations_gdf[['name', 'coordinate_value']]

    # Get all stations on the same route as the starting node
    all_stations_on_s_route = all_stations_on_matched_route(order_route_dict, s_node_on_route_gdf)
    all_stations_on_s_route_df = all_stations_on_s_route[list(all_stations_on_s_route.keys())[0]]
    all_stations_on_s_route_df['order_index'] = all_stations_on_s_route_df.index

    # Get the index of the starting node on the route
    start_node_index_on_s_route = all_stations_on_s_route_df[all_stations_on_s_route_df['name'] == start_node.iloc[0, 3]]

    # Merge stations on the same route with the transfer stations (based on name and coordinate)
    t_stations_selected = pd.merge(all_stations_on_s_route_df, t_stations_n_c, how='inner')

    # Check if the merged DataFrame is empty
    if t_stations_selected.empty:
        return print("It is impossible to travel from the starting point to the end point by only transferring once through this kind of transportation network.")
    else:
        # Calculate the absolute difference in order indices between the starting node and each transfer station
        compared_index = start_node_index_on_s_route.index.tolist()[0]
        t_stations_selected['index_diff'] = abs(t_stations_selected['order_index'] - compared_index)

        # Find the transfer station with the minimum index difference
        min_diff_index = t_stations_selected['index_diff'].idxmin()
        t_station_node = t_stations_selected.loc[[min_diff_index]]

        # Retrieve the complete information about the identified transfer station
        t_station_node = new_nodes[new_nodes['geometry'] == t_station_node.iloc[0].geometry]

        return t_station_node

        
# If the start_node and end_node are on some same routes or already get the transfer station, the next step is to get the same route----using function 's_e_same_routes'
def s_e_same_routes(s_on_route_ref, e_on_route_ref):
    """
    Check if the starting node (s_node) and ending node (e_node) are on the same route/routes.

    Parameters:
    - s_on_route_ref (pandas.DataFrame): DataFrame representing routes related to the starting node.
    - e_on_route_ref (pandas.DataFrame): DataFrame representing routes related to the ending node.

    Returns:
    pandas.DataFrame or None: DataFrame containing routes that are common to both starting and ending nodes,
    or None if there are no common routes.

    Example:
    >>> s_routes = pd.DataFrame({'id': [1, 2], 'name': ['Route A', 'Route B'], 'ref': ['A', 'B']})
    >>> e_routes = pd.DataFrame({'id': [3, 4], 'name': ['Route B', 'Route C'], 'ref': ['B', 'C']})
    >>> s_e_same_routes(s_routes, e_routes)
    s_node and e_node are on totally different routes
    The next step is to find transfer station of s_on_route:['A'] and e_on_route:['B']
    None
    """

    # Check if the references in the 'ref' column of s_on_route_ref are in the list of references in e_on_route_ref
    results = s_on_route_ref['ref'].isin(e_on_route_ref['ref'].tolist())

    # Create a DataFrame containing routes common to both s_node and e_node
    s_e_same_routes_df = s_on_route_ref[results]

    # Check if there are common routes
    if len(s_e_same_routes_df) > 0:
        print(f"s_node and e_node are on same route/routes: {s_e_same_routes_df}\n")
        return s_e_same_routes_df
    else:
        print("s_node and e_node are on totally different routes\n")
        print(f"The next step is to find transfer station of s_on_route: {s_on_route_ref['ref'].tolist()} and e_on_route: {e_on_route_ref['ref'].tolist()}")
        
        # Return None when there are no common routes
        return None

 

def s_e_same_route_gdf_metro(s_e_same_routes_df, city_sub_routes, start_node):
    """
    Finds edges with the same route as the start node and returns them as a GeoDataFrame.

    Parameters:
    -----------
    s_e_same_routes_df : pandas.DataFrame
        DataFrame containing information about edges with the same route as start node.
    city_sub_routes : pandas.DataFrame
        DataFrame containing information about city subway routes.
    start_node : pandas.DataFrame
        DataFrame containing information about the start node.

    Returns:
    --------
    gpd.GeoDataFrame
        GeoDataFrame containing edges with the same route as the start node.

    Example:
    --------
    # Define input dataframes
    s_e_same_routes_df = pd.DataFrame(...)
    city_sub_routes = pd.DataFrame(...)
    start_node = pd.DataFrame(...)

    # Call the function
    result_gdf = s_e_same_route_gdf_metro(s_e_same_routes_df, city_sub_routes, start_node)
    """
    # Merge start node edges with city sub routes
    s_e_on_possible_route_df = pd.merge(s_e_same_routes_df, city_sub_routes, on='ref', how='left')
    s_e_on_route_possible_gdf = gpd.GeoDataFrame(s_e_on_possible_route_df.copy())
    
    # Convert start node to GeoDataFrame
    start_node_gdf = gpd.GeoDataFrame(start_node.copy())
    
    # Find edges with the same route as the start node
    s_e_on_same_route_gdf = s_e_on_route_possible_gdf[s_e_on_route_possible_gdf.geometry.buffer(0.000001).intersects(start_node_gdf.iloc[0]['geometry'])]
    s_e_on_same_route_gdf = s_e_on_same_route_gdf.reset_index(drop=True)
    
    return s_e_on_same_route_gdf

    
### Get all stations between s_e nodes or s_t nodes or t_e nodes
def all_stations_on_matched_route(order_route_dict, node_on_route_gdf):
    """
    Filter stations on a route that match the provided order_route_dict.

    Parameters:
    - order_route_dict (dict): Dictionary containing order information about routes.
    - node_on_route_gdf (geopandas.GeoDataFrame): GeoDataFrame containing information about nodes on a specific route.

    Returns:
    dict: Dictionary containing stations on the route that match the provided order_route_dict.

    Example:
    >>> order_route_dict = {'StationA': df1, 'StationB': df2, ...}
    >>> node_on_route_gdf = GeoDataFrame with information about nodes on a specific route
    >>> all_stations_on_matched_route(order_route_dict, node_on_route_gdf)
    {'StationA': df1, 'StationB': df2, ...}
    """
    node_matched_all_stations_dict = {}

    # Iterate through order_route_dict items
    for key, df in order_route_dict.items():
        # Find rows in node_on_route_gdf where the 'name' column matches the key
        matched_rows = node_on_route_gdf[node_on_route_gdf['name'] == key]
        if not matched_rows.empty:
            # Add the matched rows to the dictionary
            node_matched_all_stations_dict[key] = df
    
    return node_matched_all_stations_dict
    
def all_stations_on_matched_routes(s_e_same_routes_df, sub_routes, start_node, end_node, sub_order_route_dict):
    """
    Extract all stations on routes that are common to both starting and ending nodes.

    Parameters:
    - s_e_same_routes_df (pandas.DataFrame): DataFrame containing routes common to both starting and ending nodes.
    - sub_routes (pandas.DataFrame): DataFrame containing information about sub-routes.
    - start_node (pandas.DataFrame): DataFrame containing information about the starting node.
    - end_node (pandas.DataFrame): DataFrame containing information about the ending node.
    - sub_order_route_dict (dict): Dictionary containing order information about sub-routes.

    Returns:
    list of pandas.DataFrame: List of DataFrames containing all stations on routes common to both starting and ending nodes.

    Example:
    >>> s_e_same_routes_df = DataFrame with routes common to both starting and ending nodes
    >>> sub_routes = DataFrame with information about sub-routes
    >>> start_node = DataFrame with information about the starting node
    >>> end_node = DataFrame with information about the ending node
    >>> sub_order_route_dict = {'SubRouteA': df1, 'SubRouteB': df2, ...}
    >>> all_stations_on_matched_routes(s_e_same_routes_df, sub_routes, start_node, end_node, sub_order_route_dict)
    [DataFrame1, DataFrame2, ...]
    """
    # Merge s_e_same_routes_df with sub_routes based on the 'ref' column
    s_e_same_route_df_new = pd.merge(s_e_same_routes_df, sub_routes, on='ref', how='left')

    # Create GeoDataFrames for sub_routes, s_e_node, and e_node
    sub_routes_gdf = gpd.GeoDataFrame(sub_routes.copy())
    s_e_node_gdf = gpd.GeoDataFrame(pd.concat([start_node.copy(), end_node.copy()], ignore_index=True))

    # Filter sub_routes that intersect with the geometry of the combined starting and ending nodes
    e_node_on_route_gdf = sub_routes_gdf[sub_routes_gdf.geometry.intersects(s_e_node_gdf.iloc[1].geometry)]

    # Merge s_e_same_route_df_new with e_node_on_route_gdf based on the 'ref' column
    s_e_same_route_gdf = pd.merge(s_e_same_route_df_new, e_node_on_route_gdf, how='inner')

    # Extract all stations on matched routes using sub_order_route_dict
    matched_route_all_stations_dict = all_stations_on_matched_route(sub_order_route_dict, s_e_same_route_gdf)
    all_stations_on_matched_routes_dfs = list(matched_route_all_stations_dict.values())

    return all_stations_on_matched_routes_dfs

    


def all_btw_stations_on_same_route_gdf_metro(merged_order_route_dict, s_e_on_same_route_gdf, start_node, end_node):
    """
    Finds all stations between the start and end nodes on the same route and returns them as a GeoDataFrame.

    Parameters:
    -----------
    merged_order_route_dict : dict
        Dictionary mapping route names to lists of stations in order.
    s_e_on_same_route_gdf : gpd.GeoDataFrame
        GeoDataFrame containing edges with the same route as the start node.
    start_node : pandas.DataFrame
        DataFrame containing information about the start node.
    end_node : pandas.DataFrame
        DataFrame containing information about the end node.

    Returns:
    --------
    gpd.GeoDataFrame
        GeoDataFrame containing all stations between the start and end nodes on the same route.

    Example:
    --------
    # Define input data
    merged_order_route_dict = {...}
    s_e_on_same_route_gdf = gpd.GeoDataFrame(...)
    start_node = pd.DataFrame(...)
    end_node = pd.DataFrame(...)

    # Call the function
    result_gdf = all_btw_stations_on_same_route_gdf_metro(merged_order_route_dict, s_e_on_same_route_gdf, start_node, end_node)
    """
    # Get all stations on the same route as the start node
    all_stations_on_same_route = merged_order_route_dict[s_e_on_same_route_gdf.name[0]]
    all_stations_on_same_route_gdf = gpd.GeoDataFrame(all_stations_on_same_route.copy())
    
    # Convert start and end nodes to GeoDataFrames
    start_node_gdf = gpd.GeoDataFrame(start_node.copy())
    end_node_gdf = gpd.GeoDataFrame(end_node.copy())

    # Find the index of the nearest station to the start node
    start_station_index = shapely.distance(start_node_gdf.geometry.values, all_stations_on_same_route_gdf.geometry.values).argmin()
    # Find the index of the nearest station to the end node
    end_station_index = shapely.distance(end_node_gdf.geometry.values, all_stations_on_same_route_gdf.geometry.values).argmin()
    
    # Extract stations between the start and end nodes on the same route
    all_btw_stations_on_same_route_gdf = all_stations_on_same_route_gdf.iloc[start_station_index:end_station_index+1]

    return all_btw_stations_on_same_route_gdf    




def all_stations_on_same_route_gdf_transfer(t_e_same_routes_df, city_sub_routes, city_sub_order_route_dict, t_station_node, end_node):
    """
    Finds all stations between a transfer station and an end node on the same route and returns them as a GeoDataFrame.

    Parameters:
    -----------
    t_e_same_routes_df : pandas.DataFrame
        DataFrame containing information about edges with the same route as transfer station.
    city_sub_routes : pandas.DataFrame
        DataFrame containing information about city subway routes.
    city_sub_order_route_dict : dict
        Dictionary mapping route names to lists of stations in order.
    t_station_node : pandas.DataFrame
        DataFrame containing information about transfer station node.
    end_node : pandas.DataFrame
        DataFrame containing information about the end node.

    Returns:
    --------
    gpd.GeoDataFrame
        GeoDataFrame containing all stations between the transfer station and end node on the same route.

    Raises:
    -------
    ValueError
        If no suitable stations are found between the transfer station and the end node.

    Example:
    --------
    # Define input dataframes
    t_e_same_routes_df = pd.DataFrame(...)
    city_sub_routes = pd.DataFrame(...)
    city_sub_order_route_dict = {...}
    t_station_node = pd.DataFrame(...)
    end_node = pd.DataFrame(...)

    # Call the function
    result_gdf = all_stations_on_same_route_gdf_transfer(t_e_same_routes_df, city_sub_routes, city_sub_order_route_dict, t_station_node, end_node)
    """
    # Merge transfer station edges with city sub routes
    t_e_on_possible_route_df = pd.merge(t_e_same_routes_df, city_sub_routes, on='ref', how='left')
    t_e_on_route_possible_gdf = gpd.GeoDataFrame(t_e_on_possible_route_df.copy())
    
    all_btw_stations_on_same_route_gdf_t_e = None  
    
    # Iterate over each edge on possible route
    for i in range(len(t_e_on_route_possible_gdf)):
        # Get all stations on the possible route
        all_stations_on_possible_route_t_e = city_sub_order_route_dict[t_e_on_route_possible_gdf.name[i]]
        all_stations_on_possible_route_t_e_gdf = gpd.GeoDataFrame(all_stations_on_possible_route_t_e.copy())
    
        # Convert transfer station node and end node to GeoDataFrames
        t_station_node_gdf = gpd.GeoDataFrame(t_station_node.copy())
        end_node_gdf = gpd.GeoDataFrame(end_node.copy())

        # Find the nearest station index to the transfer station node
        t_station_node_index = shapely.distance(t_station_node_gdf.geometry.values, all_stations_on_possible_route_t_e_gdf.geometry.values).argmin()
        # Find the nearest station index to the end node
        end_station_index = shapely.distance(end_node_gdf.geometry.values, all_stations_on_possible_route_t_e_gdf.geometry.values).argmin()
    
        # If transfer station index is greater than end station index, continue to next iteration
        if t_station_node_index > end_station_index:
            continue  
        else:
            # Extract stations between transfer station and end node on the same route
            all_btw_stations_on_same_route_gdf_t_e = all_stations_on_possible_route_t_e_gdf.iloc[t_station_node_index:end_station_index+1]
            break
    
    # If no suitable stations found, raise ValueError
    if all_btw_stations_on_same_route_gdf_t_e is None:
        raise ValueError("No suitable stations found.")
    
    return all_btw_stations_on_same_route_gdf_t_e



def find_nearest_station(coordinate, nodes):
    """
    Find the nearest station to a given coordinate.

    Parameters:
    - coordinate (tuple): Tuple containing latitude and longitude values.
    - nodes (geopandas.GeoDataFrame): GeoDataFrame containing information about nodes.

    Returns:
    geopandas.GeoDataFrame: GeoDataFrame containing information about the nearest station.

    Example:
    >>> coordinate = (latitude, longitude)
    >>> nodes = GeoDataFrame with information about nodes
    >>> find_nearest_station(coordinate, nodes)
    GeoDataFrame with information about the nearest station.
    """
    # Create an STRtree index for nodes and find the nearest station
    node_tree = shapely.STRtree(nodes.geometry)
    find_nearest = node_tree.nearest(shapely.points(coordinate))
    return nodes.iloc[[find_nearest]]

def between_stations_one_way(start_node, end_node, on_matched_route_df):
    """
    Extract stations between the starting and ending nodes on a one-way route.

    Parameters:
    - start_node (pandas.DataFrame): DataFrame containing information about the starting node.
    - end_node (pandas.DataFrame): DataFrame containing information about the ending node.
    - on_matched_route_df (pandas.DataFrame): DataFrame containing information about stations on a matched route.

    Returns:
    pandas.DataFrame: DataFrame containing stations between the starting and ending nodes on a one-way route.

    Example:
    >>> start_node = DataFrame with information about the starting node
    >>> end_node = DataFrame with information about the ending node
    >>> on_matched_route_df = DataFrame with information about stations on a matched route
    >>> between_stations_one_way(start_node, end_node, on_matched_route_df)
    DataFrame containing stations between the starting and ending nodes on a one-way route.
    """
    # Extract coordinates of the starting and ending nodes
    start_node_coordinate = start_node.iloc[0]['coordinate_value']
    end_node_coordinate = end_node.iloc[0]['coordinate_value']

    # Find the nearest stations to the starting and ending coordinates
    s_station_df1 = find_nearest_station(start_node_coordinate, on_matched_route_df)
    e_station_df1 = find_nearest_station(end_node_coordinate, on_matched_route_df)

    # Get the indices of the starting and ending stations
    s_index_df1 = s_station_df1.index[0]
    e_index_df1 = e_station_df1.index[0]

    # Extract stations between the starting and ending stations based on their indices
    if s_index_df1 > e_index_df1:
        s_e_between_stations = on_matched_route_df.loc[s_index_df1:e_index_df1:-1]
    else:
        s_e_between_stations = on_matched_route_df.loc[s_index_df1:e_index_df1:1]

    return s_e_between_stations

def btw_stations_each_way_list(start_node, end_node, all_stations_on_matched_routes_dfs):
    """
    Extract stations between the starting and ending nodes on each route in a list.

    Parameters:
    - start_node (pandas.DataFrame): DataFrame containing information about the starting node.
    - end_node (pandas.DataFrame): DataFrame containing information about the ending node.
    - all_stations_on_matched_routes_dfs (list): List of DataFrames containing all stations on routes common to both starting and ending nodes.

    Returns:
    list of pandas.DataFrame: List of DataFrames containing stations between the starting and ending nodes on each route.

    Example:
    >>> start_node = DataFrame with information about the starting node
    >>> end_node = DataFrame with information about the ending node
    >>> all_stations_on_matched_routes_dfs = [DataFrame1, DataFrame2, ...]
    >>> btw_stations_each_way_list(start_node, end_node, all_stations_on_matched_routes_dfs)
    [DataFrame1, DataFrame2, ...]
    """
    btw_stations_each_way_list = []

    # Iterate through all_stations_on_matched_routes_dfs
    for i in range(len(all_stations_on_matched_routes_dfs)):
        one_matched_route_df = all_stations_on_matched_routes_dfs[i]

        # Extract stations between the starting and ending nodes on each route
        df1 = between_stations_one_way(start_node, end_node, one_matched_route_df)
        btw_stations_each_way_list.append(df1)

    return btw_stations_each_way_list






    
    
# Get all id_pairs of stations between s_e nodes or s_t nodes or t_e nodes
def btw_all_ids_pairs(btw_stations_each_way_list, sub_new_nodes):
    """
    Extract pairs of node IDs between stations on each route.

    Parameters:
    - btw_stations_each_way_list (list of pandas.DataFrame): List of DataFrames containing stations between the starting and ending nodes on each route.
    - sub_new_nodes (pandas.DataFrame): DataFrame containing information about new nodes.

    Returns:
    list of pandas.DataFrame: List of DataFrames containing pairs of node IDs between stations on each route.

    Example:
    >>> btw_stations_each_way_list = [DataFrame1, DataFrame2, ...]
    >>> sub_new_nodes = DataFrame with information about new nodes
    >>> btw_all_ids_pairs(btw_stations_each_way_list, sub_new_nodes)
    [DataFrame1, DataFrame2, ...]
    """
    btw_all_id_pairs_list = []

    # Iterate through btw_stations_each_way_list
    for i in range(len(btw_stations_each_way_list)):
        # Merge sub_new_nodes with stations between the starting and ending nodes on each route
        s_e_between_nodes_way1 = sub_new_nodes.merge(btw_stations_each_way_list[i], on='coordinate_value', suffixes=('', '_drop'))
        
        # Create an empty DataFrame to store pairs of node IDs
        id_pairs_way1 = pd.DataFrame(columns=['s_id', 'e_id'])

        # Iterate through the rows of s_e_between_nodes_way1
        for i in range(len(s_e_between_nodes_way1)-1):
            # Assign the current and next node IDs to the 's_id' and 'e_id' columns, respectively
            id_pairs_way1.loc[i, 's_id'] = s_e_between_nodes_way1.loc[i, 'id']
            id_pairs_way1.loc[i, 'e_id'] = s_e_between_nodes_way1.loc[i+1, 'id']
        
        # Append the pairs of node IDs to the list
        btw_all_id_pairs_list.append(id_pairs_way1)

    return btw_all_id_pairs_list

def btw_all_ids_pairs_transfer_all(all_btw_stations_on_same_route_gdf, three_networks_nodes_transfer_all):
    s_e_between_nodes = three_networks_nodes_transfer_all.merge(all_btw_stations_on_same_route_gdf, on = 'coordinate_value', suffixes=('', '_drop'))

    btw_all_id_pairs_list = []
    s_e_btw_nodes_transfer_all = three_networks_nodes_transfer_all.merge(all_btw_stations_on_same_route_gdf, on = 'coordinate_value', suffixes=('', '_drop'))

   

    id_pairs_way1 = pd.DataFrame(columns=['s_id', 'e_id'])

  
    for i in range(len(s_e_btw_nodes_transfer_all)-1):
           
        id_pairs_way1.loc[i, 's_id'] = s_e_btw_nodes_transfer_all.loc[i, 'id']
        id_pairs_way1.loc[i, 'e_id'] = s_e_btw_nodes_transfer_all.loc[i+1, 'id']
        

    btw_all_id_pairs_list.append(id_pairs_way1)

    return btw_all_id_pairs_list

    
### Drop duplicate dataframe for id_pairs of stations between s_e nodes or s_t nodes or t_e nodes. The result explains if there are more than one way to travel between s_e nodes or s_t nodes or t_e nodes
# Generate a hash value for a DataFrame based on its values
def hash_dataframe(df):
    """
    Generate a hash value for a DataFrame based on its values.

    Parameters:
    - df (pandas.DataFrame): DataFrame for which the hash value is generated.

    Returns:
    int: Hash value representing the DataFrame.

    Example:
    >>> df = DataFrame with some data
    >>> hash_dataframe(df)
    Hash value
    """
    # Convert the values of the DataFrame to a flattened tuple and generate a hash value
    return hash(tuple(df.values.ravel()))

# Remove duplicate DataFrames from a list based on their hash values
def drop_duplicate_dataframes(df_list):
    """
    Remove duplicate DataFrames from a list based on their hash values.

    Parameters:
    - df_list (list of pandas.DataFrame): List of DataFrames to be checked for duplicates.

    Returns:
    list of pandas.DataFrame: List of unique DataFrames.

    Example:
    >>> df_list = [DataFrame1, DataFrame2, ...]
    >>> drop_duplicate_dataframes(df_list)
    [Unique DataFrame1, Unique DataFrame2, ...]
    """
    # Use a dictionary comprehension to create a dictionary with hash values as keys and DataFrames as values
    unique_dfs = {hash_dataframe(df): df for df in df_list}.values()

    # Convert the dictionary values back to a list
    return list(unique_dfs)

def drop_df_in_list(df_list):
    """
    Remove duplicate DataFrames from a list and provide a message based on their uniqueness.

    Parameters:
    - df_list (list of pandas.DataFrame): List of DataFrames to be checked for duplicates.

    Returns:
    list of pandas.DataFrame: List of unique DataFrames.

    Example:
    >>> df_list = [DataFrame1, DataFrame2, ...]
    >>> drop_df_in_list(df_list)
    [Unique DataFrame1, Unique DataFrame2, ...]
    """
    # Use drop_duplicate_dataframes to get unique DataFrames
    unique_dfs = drop_duplicate_dataframes(df_list)
    btw_all_id_pairs_list_unique = list(unique_dfs)

    # Check the length of unique_dfs and print a message based on uniqueness
    if len(unique_dfs) > 1:
        print("At least one dataframe in the list(btw_all_id_pairs_list) is different")
        return btw_all_id_pairs_list_unique
    else:
        print("All dataframes in the list are the same")
        return btw_all_id_pairs_list_unique



    

     















        
# def test_OD_metro_new(OD_df,city_sub_new_nodes,city_sub_new_edges,city_sub_routes,city_sub_order_route_dict):
    
#     for index, row in OD_df.iterrows():
#         OD_pair = pd.Series(row)
#         start_node, end_node = s_e_node_df(OD_pair, city_sub_new_nodes)
       
#         s_on_route_ref,e_on_route_ref = s_e_on_route_ref(start_node, end_node)
#         s_on_route_ref, e_on_route_ref = judge_on_route(s_on_route_ref, e_on_route_ref)
#         s_e_same_routes_df = s_e_same_routes(s_on_route_ref,e_on_route_ref)
#         if s_e_same_routes_df is None:
#             t_station_node = transfer_station_one_mode(city_sub_routes, start_node, end_node, city_sub_new_nodes, city_sub_order_route_dict)
            
#             s_on_route_ref,t_on_route_ref = s_e_on_route_ref(start_node, t_station_node)
#             s_t_same_routes_df = s_e_same_routes(s_on_route_ref,t_on_route_ref)
#             all_btw_stations_on_same_route_gdf_s_t = all_stations_on_same_route_gdf_transfer(s_t_same_routes_df,city_sub_routes,city_sub_order_route_dict,start_node,t_station_node)
#             btw_all_id_pairs_list_s_t = btw_all_ids_pairs_transfer_all(all_btw_stations_on_same_route_gdf_s_t,city_sub_new_nodes)
            
#             t_on_route_ref,e_on_route_ref = s_e_on_route_ref( t_station_node,end_node)
#             t_e_same_routes_df = s_e_same_routes(t_on_route_ref,e_on_route_ref)
#             all_btw_stations_on_same_route_gdf_t_e = all_stations_on_same_route_gdf_transfer(t_e_same_routes_df,city_sub_routes,city_sub_order_route_dict,t_station_node,end_node)
#             btw_all_id_pairs_list_t_e = btw_all_ids_pairs_transfer_all(all_btw_stations_on_same_route_gdf_t_e,city_sub_new_nodes)


        
#             btw_all_id_pairs_list_unique_s_t = drop_df_in_list(btw_all_id_pairs_list_s_t)
#             btw_all_id_pairs_list_unique_t_e = drop_df_in_list(btw_all_id_pairs_list_t_e)

#             G = create_ground_graph(city_sub_new_edges, city_sub_new_nodes)
#             shortest_path_pairs_way_s_t = all_shortest_paths(G, btw_all_id_pairs_list_unique_s_t[0], city_sub_new_edges)
#             duplicate_row_count_way_s_t, shortest_path_edges_way_s_t, edges_way_s_t = edges_with_count_weight(shortest_path_pairs_way_s_t, city_sub_new_edges)
#             shortest_path_pairs_way_t_e = all_shortest_paths(G, btw_all_id_pairs_list_unique_t_e[0], city_sub_new_edges)
#             duplicate_row_count_way_t_e, shortest_path_edges_way_t_e, edges_way_t_e = edges_with_count_weight(shortest_path_pairs_way_t_e, city_sub_new_edges)
#             shortest_path_edges_way = pd.concat([shortest_path_edges_way_s_t,shortest_path_edges_way_t_e])
        

#             fig, ax = plt.subplots(1, 1, figsize=(30, 20))
#             gpd.GeoDataFrame(city_sub_new_edges.copy()).plot(ax=ax, color='gray', alpha=0.2)
#             gpd.GeoDataFrame(shortest_path_edges_way.copy()).plot(ax=ax, zorder=1, linewidth=(shortest_path_edges_way.count_weight), color='orange')
#             print(index)
                
#         else:
#             all_btw_stations_on_same_route_gdf_s_e = all_stations_on_same_route_gdf_transfer(s_e_same_routes_df,city_sub_routes,city_sub_order_route_dict,start_node,end_node)

#             btw_all_id_pairs_list = btw_all_ids_pairs_transfer_all(all_btw_stations_on_same_route_gdf_s_e,city_sub_new_nodes)
#             btw_all_id_pairs_list_unique = drop_df_in_list(btw_all_id_pairs_list)
#             G = create_ground_graph(city_sub_new_edges, city_sub_new_nodes)
#             shortest_path_pairs_way = all_shortest_paths(G, btw_all_id_pairs_list_unique[0], city_sub_new_edges)
#             duplicate_row_count_way, shortest_path_edges_way, edges_way = edges_with_count_weight(shortest_path_pairs_way, city_sub_new_nodes)
#             fig, ax = plt.subplots(1, 1, figsize=(30, 20))
#             gpd.GeoDataFrame(city_sub_new_edges.copy()).plot(ax=ax, color='gray', alpha=0.2)
#             gpd.GeoDataFrame(shortest_path_edges_way.copy()).plot(ax=ax, zorder=1, linewidth=(shortest_path_edges_way.count_weight), color='orange')
#             print(index)
def test_OD_metro_new(OD_df, city_sub_new_nodes, city_sub_new_edges, city_sub_routes, city_sub_order_route_dict):
    """
    Test the metro journey for Origin-Destination (OD) pairs.

    Parameters:
    - OD_df (DataFrame): A DataFrame containing Origin-Destination pairs.
    - city_sub_new_nodes (DataFrame): A DataFrame containing nodes information.
    - city_sub_new_edges (DataFrame): A DataFrame containing edges information.
    - city_sub_routes (DataFrame): A DataFrame containing routes information.
    - city_sub_order_route_dict (dict): A dictionary containing ordered route information.

    Returns:
    - None

    This function iterates over each row in the OD_df DataFrame to test the metro journey for each Origin-Destination pair.
    It calculates the shortest path between the start and end nodes for each OD pair and visualizes the route on a map.

    Example:
    test_OD_metro_new(OD_df, city_sub_new_nodes, city_sub_new_edges, city_sub_routes, city_sub_order_route_dict)
    """
    
    # Iterate over each row in the OD_df DataFrame
    for index, row in OD_df.iterrows():
        # Create a pandas Series for the current row
        OD_pair = pd.Series(row)
        
        # Get start and end nodes from the OD pair using a helper function
        start_node, end_node = s_e_node_df(OD_pair, city_sub_new_nodes)
        
        # Get reference to routes that start and end at the respective nodes
        s_on_route_ref, e_on_route_ref = s_e_on_route_ref(start_node, end_node)
        
        # Ensure that start and end nodes are on valid routes
        s_on_route_ref, e_on_route_ref = judge_on_route(s_on_route_ref, e_on_route_ref)
        
        # Find routes that start and end at the same node
        s_e_same_routes_df = s_e_same_routes(s_on_route_ref, e_on_route_ref)
        
        # Check if there are no direct routes between start and end nodes
        if s_e_same_routes_df is None:
            # Find a transfer station between start and end nodes
            t_station_node = transfer_station_one_mode(city_sub_routes, start_node, end_node, city_sub_new_nodes, city_sub_order_route_dict)
            
            # Find routes that start at the start node and end at the transfer station
            s_on_route_ref, t_on_route_ref = s_e_on_route_ref(start_node, t_station_node)
            s_t_same_routes_df = s_e_same_routes(s_on_route_ref, t_on_route_ref)
            
            # Find all stations on the same route between start and transfer station
            all_btw_stations_on_same_route_gdf_s_t = all_stations_on_same_route_gdf_transfer(s_t_same_routes_df, city_sub_routes, city_sub_order_route_dict, start_node, t_station_node)
            btw_all_id_pairs_list_s_t = btw_all_ids_pairs_transfer_all(all_btw_stations_on_same_route_gdf_s_t, city_sub_new_nodes)
            
            # Find routes that start at the transfer station and end at the end node
            t_on_route_ref, e_on_route_ref = s_e_on_route_ref(t_station_node, end_node)
            t_e_same_routes_df = s_e_same_routes(t_on_route_ref, e_on_route_ref)
            
            # Find all stations on the same route between transfer station and end node
            all_btw_stations_on_same_route_gdf_t_e = all_stations_on_same_route_gdf_transfer(t_e_same_routes_df, city_sub_routes, city_sub_order_route_dict, t_station_node, end_node)
            btw_all_id_pairs_list_t_e = btw_all_ids_pairs_transfer_all(all_btw_stations_on_same_route_gdf_t_e, city_sub_new_nodes)

            # Drop duplicates in the list of station pairs
            btw_all_id_pairs_list_unique_s_t = drop_df_in_list(btw_all_id_pairs_list_s_t)
            btw_all_id_pairs_list_unique_t_e = drop_df_in_list(btw_all_id_pairs_list_t_e)

            # Create a ground graph from edges and nodes DataFrames
            G = create_ground_graph(city_sub_new_edges, city_sub_new_nodes)
            
            # Find shortest paths between all station pairs from start to transfer station
            shortest_path_pairs_way_s_t = all_shortest_paths(G, btw_all_id_pairs_list_unique_s_t[0], city_sub_new_edges)
            duplicate_row_count_way_s_t, shortest_path_edges_way_s_t, edges_way_s_t = edges_with_count_weight(shortest_path_pairs_way_s_t, city_sub_new_edges)
            
            # Find shortest paths between all station pairs from transfer station to end
            shortest_path_pairs_way_t_e = all_shortest_paths(G, btw_all_id_pairs_list_unique_t_e[0], city_sub_new_edges)
            duplicate_row_count_way_t_e, shortest_path_edges_way_t_e, edges_way_t_e = edges_with_count_weight(shortest_path_pairs_way_t_e, city_sub_new_edges)
            
            # Concatenate the shortest path edges
            shortest_path_edges_way = pd.concat([shortest_path_edges_way_s_t, shortest_path_edges_way_t_e])
            
            # Plot the routes on a map
            fig, ax = plt.subplots(1, 1, figsize=(30, 20))
            gpd.GeoDataFrame(city_sub_new_edges.copy()).plot(ax=ax, color='gray', alpha=0.2)
            gpd.GeoDataFrame(shortest_path_edges_way.copy()).plot(ax=ax, zorder=1, linewidth=(shortest_path_edges_way.count_weight), color='orange')
            print(index)
                
        else:
            # Find all stations on the same route between start and end nodes
            all_btw_stations_on_same_route_gdf_s_e = all_stations_on_same_route_gdf_transfer(s_e_same_routes_df, city_sub_routes, city_sub_order_route_dict, start_node, end_node)
            
            # Find all station pairs on the same route between start and end nodes
            btw_all_id_pairs_list = btw_all_ids_pairs_transfer_all(all_btw_stations_on_same_route_gdf_s_e, city_sub_new_nodes)
            
            # Drop duplicates in the list of station pairs
            btw_all_id_pairs_list_unique = drop_df_in_list(btw_all_id_pairs_list)
            
            # Create a ground graph from edges and nodes DataFrames
            G = create_ground_graph(city_sub_new_edges, city_sub_new_nodes)
            
            # Find shortest paths between all station pairs
            shortest_path_pairs_way = all_shortest_paths(G, btw_all_id_pairs_list_unique[0], city_sub_new_edges)
            duplicate_row_count_way, shortest_path_edges_way, edges_way = edges_with_count_weight(shortest_path_pairs_way, city_sub_new_nodes)
            
            # Plot the routes on a map
            fig, ax = plt.subplots(1, 1, figsize=(30, 20))
            gpd.GeoDataFrame(city_sub_new_edges.copy()).plot(ax=ax, color='gray', alpha=0.2)
            gpd.GeoDataFrame(shortest_path_edges_way.copy()).plot(ax=ax, zorder=1, linewidth=(shortest_path_edges_way.count_weight), color='orange')
            print(index)


# revise test_OD_metro_new to return a dictionary where the keys are the indices and the values are the shortest_path_edges_way dataframes
def test_OD_metro_new_dict(OD_df, city_sub_new_nodes, city_sub_new_edges, city_sub_routes, city_sub_order_route_dict):
    """
    Calculate shortest paths between origin-destination pairs in OD_df considering metro routes.

    Parameters:
        OD_df (pandas.DataFrame): DataFrame containing origin-destination pairs.
        city_sub_new_nodes (pandas.DataFrame): DataFrame containing node information.
        city_sub_new_edges (pandas.DataFrame): DataFrame containing edge information.
        city_sub_routes (pandas.DataFrame): DataFrame containing metro routes.
        city_sub_order_route_dict (dict): Dictionary mapping route orders to routes.

    Returns:
        dict: A dictionary where keys are the indices and values are the shortest_path_edges_way dataframes.
    """

    # Initialize an empty dictionary to store results
    OD_edges_result_dict = {}

    for index, row in OD_df.iterrows():
        # Get origin-destination pair
        OD_pair = pd.Series(row)
        start_node, end_node = s_e_node_df(OD_pair, city_sub_new_nodes)

        # Check if start and end nodes are on the same route
        s_on_route_ref, e_on_route_ref = s_e_on_route_ref(start_node, end_node)
        s_on_route_ref, e_on_route_ref = judge_on_route(s_on_route_ref, e_on_route_ref)
        s_e_same_routes_df = s_e_same_routes(s_on_route_ref, e_on_route_ref)

        if s_e_same_routes_df is None:
            # If not on the same route, find shortest path via transfer station
            t_station_node = transfer_station_one_mode(city_sub_routes, start_node, end_node, city_sub_new_nodes, city_sub_order_route_dict)

            # Find shortest paths from start to transfer station and transfer station to end
            s_on_route_ref, t_on_route_ref = s_e_on_route_ref(start_node, t_station_node)
            s_t_same_routes_df = s_e_same_routes(s_on_route_ref, t_on_route_ref)
            all_btw_stations_on_same_route_gdf_s_t = all_stations_on_same_route_gdf_transfer(s_t_same_routes_df, city_sub_routes, city_sub_order_route_dict, start_node, t_station_node)
            btw_all_id_pairs_list_s_t = btw_all_ids_pairs_transfer_all(all_btw_stations_on_same_route_gdf_s_t, city_sub_new_nodes)

            t_on_route_ref, e_on_route_ref = s_e_on_route_ref(t_station_node, end_node)
            t_e_same_routes_df = s_e_same_routes(t_on_route_ref, e_on_route_ref)
            all_btw_stations_on_same_route_gdf_t_e = all_stations_on_same_route_gdf_transfer(t_e_same_routes_df, city_sub_routes, city_sub_order_route_dict, t_station_node, end_node)
            btw_all_id_pairs_list_t_e = btw_all_ids_pairs_transfer_all(all_btw_stations_on_same_route_gdf_t_e, city_sub_new_nodes)

            # Combine unique pairs of start-transfer and transfer-end stations
            btw_all_id_pairs_list_unique_s_t = drop_df_in_list(btw_all_id_pairs_list_s_t)
            btw_all_id_pairs_list_unique_t_e = drop_df_in_list(btw_all_id_pairs_list_t_e)

            # Create ground graph
            G = create_ground_graph(city_sub_new_edges, city_sub_new_nodes)

            # Find shortest paths from start to transfer station and transfer station to end
            shortest_path_pairs_way_s_t = all_shortest_paths(G, btw_all_id_pairs_list_unique_s_t[0], city_sub_new_edges)
            duplicate_row_count_way_s_t, shortest_path_edges_way_s_t, edges_way_s_t = edges_with_count_weight(shortest_path_pairs_way_s_t, city_sub_new_edges)
            shortest_path_pairs_way_t_e = all_shortest_paths(G, btw_all_id_pairs_list_unique_t_e[0], city_sub_new_edges)
            duplicate_row_count_way_t_e, shortest_path_edges_way_t_e, edges_way_t_e = edges_with_count_weight(shortest_path_pairs_way_t_e, city_sub_new_edges)

            # Combine paths from start to transfer and transfer to end
            shortest_path_edges_way = pd.concat([shortest_path_edges_way_s_t, shortest_path_edges_way_t_e])

        else:
            # If on the same route, find shortest path directly
            all_btw_stations_on_same_route_gdf_s_e = all_stations_on_same_route_gdf_transfer(s_e_same_routes_df, city_sub_routes, city_sub_order_route_dict, start_node, end_node)
            btw_all_id_pairs_list = btw_all_ids_pairs_transfer_all(all_btw_stations_on_same_route_gdf_s_e, city_sub_new_nodes)
            btw_all_id_pairs_list_unique = drop_df_in_list(btw_all_id_pairs_list)

            # Create ground graph
            G = create_ground_graph(city_sub_new_edges, city_sub_new_nodes)

            # Find shortest paths
            shortest_path_pairs_way = all_shortest_paths(G, btw_all_id_pairs_list_unique[0], city_sub_new_edges)
            duplicate_row_count_way, shortest_path_edges_way, edges_way = edges_with_count_weight(shortest_path_pairs_way, city_sub_new_nodes)

        # Store the result in the dictionary
        OD_edges_result_dict[index] = shortest_path_edges_way.copy()
        print(index)

    return OD_edges_result_dict










# convert coordinates in station_df from degree to meter
# def convert_deg_to_meter(new_nodes_df):
#     # Convert the 'geometry' column to a Point object
#     new_nodes_df['geometry'] = new_nodes_df.apply(lambda row: Point(row['geo_x'], row['geo_y']), axis=1)
    
#     # Define the coordinate reference systems (CRS)
#     crs_deg = 'EPSG:4326'  # WGS 84, the standard coordinate system for GPS
#     crs_meter = 'EPSG:3857'  # World Mercator, commonly used for distance calculations

#     # Create a transformer to convert from degrees to meters
#     transformer = Transformer.from_crs(crs_deg, crs_meter, always_xy=True)

#     # Apply the transformer to each Point in the 'geometry' column
#     new_nodes_df['geometry'] = new_nodes_df['geometry'].apply(lambda point: Point(transformer.transform(point.x, point.y)))
    
#     return new_nodes_df
def convert_deg_to_meter(new_nodes_df):
    """
    Convert latitude and longitude coordinates from degrees to meters.

    Parameters:
    - new_nodes_df (DataFrame): A DataFrame containing latitude ('geo_x') and longitude ('geo_y') coordinates.

    Returns:
    - DataFrame: A DataFrame with 'geometry' column converted to Point objects in meters.

    This function converts latitude and longitude coordinates from degrees to meters using the World Mercator (EPSG:3857) coordinate system,
    which is commonly used for distance calculations. It first creates a Point object for each coordinate pair, then transforms these
    points from the standard WGS 84 (EPSG:4326) coordinate system to meters using a coordinate transformer. Finally, it returns the DataFrame
    with the 'geometry' column converted to Point objects in meters.

    Example:
    new_nodes_df_meter = convert_deg_to_meter(new_nodes_df)
    """
    # Convert the 'geometry' column to a Point object
    new_nodes_df['geometry'] = new_nodes_df.apply(lambda row: Point(row['geo_x'], row['geo_y']), axis=1)
    
    # Define the coordinate reference systems (CRS)
    crs_deg = 'EPSG:4326'  # WGS 84, the standard coordinate system for GPS
    crs_meter = 'EPSG:3857'  # World Mercator, commonly used for distance calculations

    # Create a transformer to convert from degrees to meters
    transformer = Transformer.from_crs(crs_deg, crs_meter, always_xy=True)

    # Apply the transformer to each Point in the 'geometry' column
    new_nodes_df['geometry'] = new_nodes_df['geometry'].apply(lambda point: Point(transformer.transform(point.x, point.y)))
    
    return new_nodes_df


# def new_nodes_buffer(new_nodes_df,buffer_radius):
#     new_nodes_df = convert_deg_to_meter(new_nodes_df)
#     node_buffers = []

#     for index, row in new_nodes_df.iterrows():
#         point = row['geometry']
#         buffer = point.buffer(buffer_radius)
#         node_buffers.append(buffer)

#     buffered_gdf = gpd.GeoDataFrame(geometry=node_buffers)
#     buffered_df = pd.DataFrame(buffered_gdf).rename(columns={'geometry': 'buffer_geometry'})
#     new_nodes_buffer_geometry = pd.concat([new_nodes_df,buffered_df],axis=1)
#     return new_nodes_buffer_geometry    
def new_nodes_buffer(new_nodes_df, buffer_radius):
    """
    Create buffer polygons around new nodes in a GeoDataFrame.

    Parameters:
        new_nodes_df (GeoDataFrame): A GeoDataFrame containing the new nodes with their geometry.
        buffer_radius (float): The radius of the buffer around each new node, in meters.

    Returns:
        new_nodes_buffer_geometry (DataFrame): A DataFrame containing the original new nodes data 
                                                along with buffer polygons geometry.

    Example:
        new_nodes_df = gpd.GeoDataFrame({
            'id': [1, 2, 3],
            'geometry': [Point(10, 20), Point(15, 25), Point(20, 30)]
        })
        buffer_radius = 100  # buffer radius in meters

        new_nodes_buffer_geometry = new_nodes_buffer(new_nodes_df, buffer_radius)

    Explanation:
        - This function creates buffer polygons around new nodes in a GeoDataFrame.
        - It first converts the latitude and longitude coordinates of new nodes to meters using convert_deg_to_meter function.
        - Then, it iterates over each row in the new_nodes_df GeoDataFrame.
        - For each new node, it creates a buffer polygon around the node's geometry with the specified buffer radius.
        - The buffer polygons are stored in a list called node_buffers.
        - These buffer polygons are converted into a GeoDataFrame called buffered_gdf.
        - The buffered_gdf is then converted to a DataFrame called buffered_df, renaming the geometry column to 'buffer_geometry'.
        - The original new_nodes_df and buffered_df are concatenated along the columns axis to create new_nodes_buffer_geometry DataFrame.
        - Finally, the new_nodes_buffer_geometry DataFrame containing the original new nodes data along with buffer polygons geometry is returned.
    """
    # Convert latitude and longitude coordinates of new nodes to meters
    new_nodes_df = convert_deg_to_meter(new_nodes_df)
    # Initialize an empty list to store buffer polygons
    node_buffers = []

    # Iterate over each row in the new_nodes_df GeoDataFrame
    for index, row in new_nodes_df.iterrows():
        # Get the point geometry of the new node
        point = row['geometry']
        # Create a buffer polygon around the point geometry with the specified buffer radius
        buffer = point.buffer(buffer_radius)
        # Add the buffer polygon to the node_buffers list
        node_buffers.append(buffer)

    # Convert the list of buffer polygons to a GeoDataFrame
    buffered_gdf = gpd.GeoDataFrame(geometry=node_buffers)
    # Convert the buffered_gdf to a DataFrame, renaming the geometry column to 'buffer_geometry'
    buffered_df = pd.DataFrame(buffered_gdf).rename(columns={'geometry': 'buffer_geometry'})
    # Concatenate the original new_nodes_df and buffered_df along the columns axis
    new_nodes_buffer_geometry = pd.concat([new_nodes_df, buffered_df], axis=1)
    
    # Return the DataFrame containing the original new nodes data along with buffer polygons geometry
    return new_nodes_buffer_geometry


def add_footnote_to_new_nodes(new_nodes_df):
    new_nodes_df = convert_deg_to_meter(new_nodes_df)
    # create a exactly same dataframe with new_nodes_df but name columns by different transportation routes
    rename_cols = {
    "geometry": "geometry"+"_"+new_nodes_df.loc[0,'route'],
    "degree": "degree"+"_"+new_nodes_df.loc[0,'route'],
    "id": "id"+"_"+new_nodes_df.loc[0,'route'],
    "name": "name"+"_"+new_nodes_df.loc[0,'route'],
    "geo_x": "geo_x"+"_"+new_nodes_df.loc[0,'route'],
    "geo_y": "geo_y"+"_"+new_nodes_df.loc[0,'route'],
    "coordinate_value": "coordinate_value"+"_"+new_nodes_df.loc[0,'route'],
    "route_name_list": "route_name_list"+"_"+new_nodes_df.loc[0,'route'],
    "route": "route"+"_"+new_nodes_df.loc[0,'route'],
    "ref": "ref"+"_"+new_nodes_df.loc[0,'route'],
    "transfer": "transfer"+"_"+new_nodes_df.loc[0,'route']
    }
    new_nodes_ftn = new_nodes_df.rename(columns=rename_cols)
    return new_nodes_ftn


def transfer_A_to_B(A_new_nodes_buffer_geometry,B_new_nodes_ftn):
    transfer_stations_A_to_B = pd.DataFrame()
    
    for index, row in A_new_nodes_buffer_geometry.iterrows():
        each_transfer_stations_df = B_new_nodes_ftn[row['buffer_geometry'].intersects(B_new_nodes_ftn.iloc[:, 1])]
        
        if not each_transfer_stations_df.empty:
            replicated_row = pd.concat([row] * len(each_transfer_stations_df), axis=1).transpose()
            replicated_row.reset_index(drop=True, inplace=True)  # Reset the index
            each_transfer_stations_df.reset_index(drop=True, inplace=True)  # Reset the index
            co_df = pd.concat([replicated_row,each_transfer_stations_df], axis=1)
            transfer_stations_A_to_B = pd.concat([transfer_stations_A_to_B,co_df])

    transfer_stations_A_to_B = transfer_stations_A_to_B.sort_values(by='name')
    transfer_stations_A_to_B.reset_index(drop=True,inplace=True)
       

    return transfer_stations_A_to_B


def process_ref_transfer_all(cell_value):
    if pd.notna(cell_value):  # 检查是否是 NaN 值
        return set(filter(lambda x: x != 'nan', map(str.strip, cell_value.split(','))))
    else:
        return set()


def add_ref_transfer_all_to_new_nodes(transfer_stations_A_to_B, transfer_stations_A_to_C, A_new_nodes):

    selected_columns_to_B = transfer_stations_A_to_B.iloc[:, [0, 3, 9,21]]

    unique_rows_to_B = selected_columns_to_B.drop_duplicates(subset='geometry')

    unique_rows_to_B['ref_1'] = unique_rows_to_B.apply(lambda row: f"{row.iloc[2]}, {row.iloc[3]}", axis=1)


    transfer_ref_to_B = unique_rows_to_B[['name', 'ref_1']].drop_duplicates(subset='name')

    A_new_nodes_to_B = pd.merge(A_new_nodes,transfer_ref_to_B,on = 'name', how = 'left')

    
    selected_columns_to_C = transfer_stations_A_to_C.iloc[:, [0, 3, 9,21]]

    unique_rows_to_C = selected_columns_to_C.drop_duplicates(subset='geometry')

    unique_rows_to_C['ref_2'] = unique_rows_to_C.apply(lambda row: f"{row.iloc[2]}, {row.iloc[3]}", axis=1)


    transfer_ref_to_C = unique_rows_to_C[['name', 'ref_2']].drop_duplicates(subset='name')

    A_new_nodes_to_B_C = pd.merge(A_new_nodes_to_B,transfer_ref_to_C,on = 'name', how = 'left')
    

    A_new_nodes_to_B_C['ref_transfer_all'] = A_new_nodes_to_B_C.apply(lambda row: f"{row['ref']}, {row['ref_1']}, {row['ref_2']}", axis=1)
    
    A_new_nodes_to_B_C['ref_transfer_all'] = A_new_nodes_to_B_C['ref_transfer_all'].apply(process_ref_transfer_all)

    A_new_nodes_transfer_all = A_new_nodes_to_B_C
    

    return A_new_nodes_transfer_all


# def create_connect_edges(transfer_stations_sub_to_tram):
    
#     lines = []
#     from_id_list = []
#     to_id_list = []


#     for index, row in transfer_stations_sub_to_tram.iterrows():
#         line = LineString([list(row.iloc[6]), list(row.iloc[18])])
#         from_id = row.iloc[0]
#         to_id = row.iloc[12]
    
#         lines.append(line)
#         from_id_list.append(from_id)
#         to_id_list.append(to_id)

#     result_df = pd.DataFrame({'geometry': lines,
#                               'from_id':from_id_list,
#                               'to_id':to_id_list})
#     result_df['from_to'] = list(zip(result_df.from_id,result_df.to_id))
#     result_df['to_from'] = list(zip(result_df.to_id,result_df.from_id))
#     return result_df
def create_connect_edges(transfer_stations_sub_to_tram):
    
    lines1 = []
    from_id_list1 = []
    to_id_list1 = []


    for index, row in transfer_stations_sub_to_tram.iterrows():
        line = LineString([list(row.iloc[6]), list(row.iloc[18])])
        from_id = row.iloc[0]
        to_id = row.iloc[12]
    
        
        lines1.append(line)
        from_id_list1.append(from_id)
        to_id_list1.append(to_id)

    result_df1 = pd.DataFrame({'geometry': lines1,
                              'from_id':from_id_list1,
                              'to_id':to_id_list1})


    lines2 = []
    from_id_list2 = []
    to_id_list2 = []


    for index, row in transfer_stations_sub_to_tram.iterrows():
        line = LineString([list(row.iloc[18]), list(row.iloc[6])])
        from_id = row.iloc[12]
        to_id = row.iloc[0]
    
        
        lines2.append(line)
        from_id_list2.append(from_id)
        to_id_list2.append(to_id)

    result_df2 = pd.DataFrame({'geometry': lines2,
                              'from_id':from_id_list2,
                              'to_id':to_id_list2}) 

    result_df = pd.concat([result_df1,result_df2])
    result_df['from_to'] = list(zip(result_df.from_id,result_df.to_id))
    result_df['to_from'] = list(zip(result_df.to_id,result_df.from_id))
    return result_df





def select_connect_stations_one_network(city_sub_new_stations):
    """
    Selects stations that connect four different lines from a DataFrame of subway stations.

    Parameters:
        city_sub_new_stations (pandas.DataFrame): DataFrame containing subway station data.

    Returns:
        tuple: A tuple containing a list of station names and a DataFrame of the selected stations.
    """

    # Select stations with transfer information available
    city_sub_transfer_stations = city_sub_new_stations[city_sub_new_stations['transfer'].notna()]

    # Sort the transfer stations by name
    city_sub_transfer_stations = city_sub_transfer_stations.sort_values(by='name')

    # Group stations by name and count occurrences
    grouped = city_sub_transfer_stations.groupby('name').size()

    # Select station names that connect four different lines
    connect_stations_name = grouped[grouped == 4].index.tolist()

    # Filter DataFrame to include only the selected station names
    connect_stations_dataframe = city_sub_transfer_stations[city_sub_transfer_stations['name'].isin(connect_stations_name)]

    # Return the list of station names and the filtered DataFrame
    return connect_stations_name, connect_stations_dataframe


# def create_connect_edges_one_network(connect_stations, id_edges_length_sub, city_sub_new_edges):
#     """
#     Create connecting edges between stations in a network.

#     Parameters:
#         connect_stations (DataFrame): A DataFrame containing information about connecting stations.
#         id_edges_length_sub (int): The starting ID number for the connecting edges.
#         city_sub_new_edges (DataFrame): A DataFrame containing information about existing edges in the network.

#     Returns:
#         connect_edges (DataFrame): A DataFrame containing information about the connecting edges.

#     Example:
#         connect_stations = pd.DataFrame({
#             'id': [1, 2, 3],
#             'coordinate_value': [(10, 20), (15, 25), (20, 30)]
#         })
#         id_edges_length_sub = 100  # starting ID number for connecting edges
#         city_sub_new_edges = pd.DataFrame(...)  # DataFrame containing existing edges in the network

#         connect_edges = create_connect_edges_one_network(connect_stations, id_edges_length_sub, city_sub_new_edges)

#     Explanation:
#         - This function creates connecting edges between stations in a network.
#         - It generates all possible pairs of stations from the connect_stations DataFrame.
#         - For each pair of stations, it creates a LineString representing the edge between them.
#         - The information about these connecting edges is stored in the connect_edges DataFrame.
#         - Each connecting edge is assigned a unique ID number starting from id_edges_length_sub.
#         - The weights and time attributes of the connecting edges are set to values higher than the maximum values in the city_sub_new_edges DataFrame.
#         - Finally, the connect_edges DataFrame containing information about the connecting edges is returned.
#     """
#     # Extract IDs of connecting stations
#     id_list = list(connect_stations['id'])
#     # Generate all possible pairs of station IDs
#     id_pairs_list = list(permutations(id_list, 2))

#     # Extract coordinates of connecting stations
#     connect_station_coordinate_list = list(connect_stations['coordinate_value'])
#     # Create LineString segments for connecting edges
#     line_segments = [LineString(pair) for pair in permutations(connect_station_coordinate_list, 2)]

#     # Create a DataFrame to store information about connecting edges
#     connect_edges = pd.DataFrame({
#         'from_id': [pair[0] for pair in id_pairs_list],
#         'to_id': [pair[1] for pair in id_pairs_list],
#         'from_to': id_pairs_list,
#         'to_from': [(pair[1], pair[0]) for pair in id_pairs_list],
#         'geometry': line_segments
#     })

#     # Assign unique IDs to connecting edges
#     connect_edges_id_list = [id_edges_length_sub] + [i for i in range(id_edges_length_sub + 1, id_edges_length_sub + len(connect_edges))]
#     connect_edges['id'] = connect_edges_id_list
    
#     # Set weights attribute of connecting edges to a value higher than the maximum value in city_sub_new_edges
#     connect_edges['weights'] = city_sub_new_edges.sort_values(by='weights').weights.iloc[-1] + 1
#     # Set time attribute of connecting edges to a value higher than the maximum value in city_sub_new_edges
#     connect_edges['time'] = city_sub_new_edges.sort_values(by='time').time.iloc[-1] + 0.1
    
#     # Return the DataFrame containing information about the connecting edges
    # return connect_edges

# def create_connect_edges_one_network(connect_stations, id_edges_length_sub, city_sub_new_edges):
#     """
#     Create connecting edges between stations in a network.

#     Parameters:
#         connect_stations (DataFrame): A DataFrame containing information about connecting stations.
#         id_edges_length_sub (int): The starting ID number for the connecting edges.
#         city_sub_new_edges (DataFrame): A DataFrame containing information about existing edges in the network.

#     Returns:
#         connect_edges (DataFrame): A DataFrame containing information about the connecting edges.

#     Example:
#         connect_stations = pd.DataFrame({
#             'id': [1, 2, 3],
#             'coordinate_value': [(10, 20), (15, 25), (20, 30)]
#         })
#         id_edges_length_sub = 100  # starting ID number for connecting edges
#         city_sub_new_edges = pd.DataFrame(...)  # DataFrame containing existing edges in the network

#         connect_edges = create_connect_edges_one_network(connect_stations, id_edges_length_sub, city_sub_new_edges)

#     Explanation:
#         - This function creates connecting edges between stations in a network.
#         - It generates all possible pairs of stations from the connect_stations DataFrame.
#         - For each pair of stations, it creates a LineString representing the edge between them.
#         - The information about these connecting edges is stored in the connect_edges DataFrame.
#         - Each connecting edge is assigned a unique ID number starting from id_edges_length_sub.
#         - The weights and time attributes of the connecting edges are set to values higher than the maximum values in the city_sub_new_edges DataFrame.
#         - Finally, the connect_edges DataFrame containing information about the connecting edges is returned.
#     """
#     # Extract IDs of connecting stations
#     id_list = list(connect_stations['id'])
#     # Generate all possible pairs of station IDs
#     id_pairs_list = list(permutations(id_list, 2))

#     # Extract coordinates of connecting stations
#     connect_station_coordinate_list = list(connect_stations['coordinate_value'])
#     # Create LineString segments for connecting edges
#     line_segments = [LineString(pair) for pair in permutations(connect_station_coordinate_list, 2)]
    
#     connect_stations = gpd.GeoDataFrame(connect_stations, geometry='geometry', crs='EPSG:4326')
#     # Define the coordinate reference systems (CRS)
#     crs_deg = 'EPSG:4326'  # WGS 84, the standard coordinate system for GPS
#     crs_meter = 'EPSG:3857'  # World Mercator, commonly used for distance calculations

#     # Create a transformer to convert from degrees to meters
#     transformer = Transformer.from_crs(crs_deg, crs_meter, always_xy=True)

#     # Apply the transformer to each Point in the 'geometry' column
#     connect_stations['geometry_3857'] = connect_stations['geometry'].apply(lambda point: Point(transformer.transform(point.x, point.y)))
#     connect_stations_geometry_list = list(connect_stations['geometry_3857'])
#     distance_list = [LineString(pair).length for pair in permutations(connect_stations_geometry_list, 2)]
    

#     # Create a DataFrame to store information about connecting edges
#     connect_edges = pd.DataFrame({
#         'from_id': [pair[0] for pair in id_pairs_list],
#         'to_id': [pair[1] for pair in id_pairs_list],
#         'from_to': id_pairs_list,
#         'to_from': [(pair[1], pair[0]) for pair in id_pairs_list],
#         'geometry': line_segments,
#         'distance': distance_list
#     })

#     # Assign unique IDs to connecting edges
#     connect_edges_id_list = [id_edges_length_sub] + [i for i in range(id_edges_length_sub + 1, id_edges_length_sub + len(connect_edges))]
#     connect_edges['id'] = connect_edges_id_list
    
#     # Set weights attribute of connecting edges to a value higher than the maximum value in city_sub_new_edges
#     connect_edges['weights'] = city_sub_new_edges.sort_values(by='weights').weights.iloc[-1] + 1
#     # Set time attribute of connecting edges to a value higher than the maximum value in city_sub_new_edges
#     connect_edges['time'] = city_sub_new_edges.sort_values(by='time').time.iloc[-1] + 0.1
#     connect_edges = connect_edges[connect_edges['distance'] <= 500 ]
   
#     # Return the DataFrame containing information about the connecting edges
#     return connect_edges
    
def create_connect_edges_one_network(connect_stations, id_edges_length_sub, city_sub_new_edges):
    """
    Create connecting edges between stations in a network.

    Parameters:
        connect_stations (DataFrame): A DataFrame containing information about connecting stations.
        id_edges_length_sub (int): The starting ID number for the connecting edges.
        city_sub_new_edges (DataFrame): A DataFrame containing information about existing edges in the network.

    Returns:
        connect_edges (DataFrame): A DataFrame containing information about the connecting edges.

    Example:
        connect_stations = pd.DataFrame({
            'id': [1, 2, 3],
            'coordinate_value': [(10, 20), (15, 25), (20, 30)]
        })
        id_edges_length_sub = 100  # starting ID number for connecting edges
        city_sub_new_edges = pd.DataFrame(...)  # DataFrame containing existing edges in the network

        connect_edges = create_connect_edges_one_network(connect_stations, id_edges_length_sub, city_sub_new_edges)

    Explanation:
        - This function creates connecting edges between stations in a network.
        - It generates all possible pairs of stations from the connect_stations DataFrame.
        - For each pair of stations, it creates a LineString representing the edge between them.
        - The information about these connecting edges is stored in the connect_edges DataFrame.
        - Each connecting edge is assigned a unique ID number starting from id_edges_length_sub.
        - The weights and time attributes of the connecting edges are set to values higher than the maximum values in the city_sub_new_edges DataFrame.
        - Finally, the connect_edges DataFrame containing information about the connecting edges is returned.
    """
    # Extract IDs of connecting stations
    id_list = list(connect_stations['id'])
    # Generate all possible pairs of station IDs
    id_pairs_list = list(permutations(id_list, 2))

    # Extract coordinates of connecting stations
    connect_station_coordinate_list = list(connect_stations['coordinate_value'])
    # Create LineString segments for connecting edges
    line_segments = [LineString(pair) for pair in permutations(connect_station_coordinate_list, 2)]
    
    connect_stations = gpd.GeoDataFrame(connect_stations, geometry='geometry', crs='EPSG:4326')
    # Define the coordinate reference systems (CRS)
    crs_deg = 'EPSG:4326'  # WGS 84, the standard coordinate system for GPS
    crs_meter = 'EPSG:3857'  # World Mercator, commonly used for distance calculations

    # Create a transformer to convert from degrees to meters
    transformer = Transformer.from_crs(crs_deg, crs_meter, always_xy=True)

    # Apply the transformer to each Point in the 'geometry' column
    connect_stations['geometry_3857'] = connect_stations['geometry'].apply(lambda point: Point(transformer.transform(point.x, point.y)))
    connect_stations_geometry_list = list(connect_stations['geometry_3857'])
    distance_list = [LineString(pair).length for pair in permutations(connect_stations_geometry_list, 2)]
    

    # Create a DataFrame to store information about connecting edges
    connect_edges = pd.DataFrame({
        'from_id': [pair[0] for pair in id_pairs_list],
        'to_id': [pair[1] for pair in id_pairs_list],
        'from_to': id_pairs_list,
        'to_from': [(pair[1], pair[0]) for pair in id_pairs_list],
        'geometry': line_segments,
        'distance': distance_list
    })
    
    # Only keep the links which are less than 500meters
    connect_edges = connect_edges[connect_edges['distance'] <= 500].reset_index(drop=True)
    
    # Assign unique IDs to connecting edges
    connect_edges_id_list = [id_edges_length_sub] + [i for i in range(id_edges_length_sub + 1, id_edges_length_sub + len(connect_edges))]
    connect_edges['id'] = connect_edges_id_list
    
    # Set weights attribute of connecting edges to a value much more higher than the maximum value in city_sub_new_edges to avoid frequently transfer: 15 min/60 min*25000 m/h
    # connect_edges['weights'] = city_sub_new_edges.sort_values(by='weights').weights.iloc[-1] + 1
    # connect_edges['weights'] = city_sub_new_edges.sort_values(by='weights').weights.iloc[-1] + 15/60*25000
    connect_edges['weights'] = city_sub_new_edges.sort_values(by='distance').distance.iloc[-1] + 15/60*25000
    
    # Set time attribute of connecting edges to a value higher than the maximum value in city_sub_new_edges: 15mins for passengers' walking transfer
    # connect_edges['time'] = city_sub_new_edges.sort_values(by='time').time.iloc[-1] +  1
    connect_edges['time'] = 15
    connect_edges['wcl_weights'] = 300
    
    # Return the DataFrame containing information about the connecting edges
    return connect_edges


def connected_all_edges_dataframe(connect_stations_name, city_sub_new_stations, id_edges_length_sub, city_sub_new_edges):
    """
    Creates a DataFrame of connected edges between subway stations.

    Parameters:
        connect_stations_name (list): List of station names that connect different lines.
        city_sub_new_stations (pandas.DataFrame): DataFrame containing subway station data.
        id_edges_length_sub (int): Initial ID for edges in the subway network.
        city_sub_new_edges (pandas.DataFrame): DataFrame containing edges data.

    Returns:
        pandas.DataFrame: DataFrame of connected edges between subway stations.
    """
    connect_edges_dfs = {}
    # Iterate over each connecting station
    for i in range(len(connect_stations_name)):
        # Select DataFrame for the current connecting station
        connect_stations_df = city_sub_new_stations[city_sub_new_stations['name'] == connect_stations_name[i]]

        # Create edges for the current connecting station
        if i == 0:
            connect_edges = create_connect_edges_one_network(connect_stations_df, id_edges_length_sub, city_sub_new_edges)
            city_sub_connected_edges = pd.concat([city_sub_new_edges, connect_edges]).reset_index(drop=True)
            id_connected_edges_length_sub = id_edges_length_sub + len(connect_edges)
            # print(connect_edges)
        else:
            connect_edges = create_connect_edges_one_network(connect_stations_df, id_connected_edges_length_sub, city_sub_new_edges)
            city_sub_connected_edges = pd.concat([city_sub_connected_edges, connect_edges]).reset_index(drop=True)
            id_connected_edges_length_sub = id_connected_edges_length_sub + len(connect_edges)
            # print(connect_edges)
        connect_edges_dfs[connect_stations_name[i]] = connect_edges
        city_sub_connected_edges['weights'] = city_sub_connected_edges['weights'].fillna(city_sub_connected_edges['distance'])


    return city_sub_connected_edges, connect_edges_dfs




    






def walking_linear_distance(start_end_points_coordinates_pairs,start_end_nearest_id_pairs,new_nodes):

    # unite is meter
    start_point_tuple = start_end_points_coordinates_pairs.iloc[0]['s_coordinates']
    end_point_tuple = start_end_points_coordinates_pairs.iloc[0]['e_coordinates']

    start_nodes_df = pd.DataFrame(new_nodes.loc[new_nodes['id'] == start_end_nearest_id_pairs.iloc[0]['s_id']])
    end_nodes_df = pd.DataFrame(new_nodes.loc[new_nodes['id'] == start_end_nearest_id_pairs.iloc[0]['e_id']])
    start_nodes_tuple = (start_nodes_df.iloc[0]['geometry'].x, start_nodes_df.iloc[0]['geometry'].y)
    end_nodes_tuple = (end_nodes_df.iloc[0]['geometry'].x, end_nodes_df.iloc[0]['geometry'].y)

    distance_s_s_tuple = geodesic(start_point_tuple, start_nodes_tuple).meters
    distance_e_e_tuple = geodesic(end_point_tuple, end_nodes_tuple).meters
    
    return distance_s_s_tuple,distance_e_e_tuple

def transfer_using_time(short_path_edges,distance_s_s_tuple,distance_e_e_tuple):
    
    time_on_transport = short_path_edges['time'].sum()
    speed_of_walking = 5000
    time_of_walking = distance_s_s_tuple/speed_of_walking + distance_e_e_tuple/speed_of_walking
    using_time_hour = time_on_transport + time_of_walking
    using_time_minutes = round(using_time_hour * 60)
    
    return using_time_hour,using_time_minutes,time_on_transport,time_of_walking


def compare_using_time_st(choosesub_using_time_tuple, sub_new_edges, sub_short_path_edges,
                       choosetram_using_time_tuple, tram_new_edges, tram_short_path_edges):
    
    using_time_subway = choosesub_using_time_tuple[0]
    using_time_tram = choosetram_using_time_tuple[0]
    
    min_using_time = min(using_time_subway, using_time_tram)

    if min_using_time == using_time_subway:
        dict_fastest = {'using_time': using_time_subway,
                 'new_edges': sub_new_edges,
                 'shortest_path_edges': sub_short_path_edges
                }
    else:
        dict_fastest = {'using_time': using_time_tram,
                 'new_edges': tram_new_edges,
                 'shortest_path_edges': tram_short_path_edges
                }

    return dict_fastest    

def compare_using_time_stb(choosesub_using_time_tuple, sub_new_edges, sub_short_path_edges,
                       choosetram_using_time_tuple, tram_new_edges, tram_short_path_edges,
                       choosebus_using_time_tuple, bus_new_edges, bus_short_path_edges):
    
    using_time_subway = choosesub_using_time_tuple[0]
    using_time_tram = choosetram_using_time_tuple[0]
    using_time_bus = choosebus_using_time_tuple[0]
    
    min_using_time = min(using_time_subway, using_time_tram, using_time_bus)

    if min_using_time == using_time_subway:
        dict_fastest = {'using_time': using_time_subway,
                 'new_edges': sub_new_edges,
                 'shortest_path_edges': sub_short_path_edges
                }
    elif min_using_time == using_time_tram:
        dict_fastest = {'using_time': using_time_tram,
                 'new_edges': tram_new_edges,
                 'shortest_path_edges': tram_short_path_edges
                }
    else:
        dict_fastest = {'using_time': using_time_bus,
                 'new_edges': bus_new_edges,
                 'shortest_path_edges': bus_short_path_edges
                }

    return dict_fastest
                           
def plot_chosen_single_transport(c_new_edges,c_short_path_edges):

    fig, ax = plt.subplots(1, 1, figsize=(30, 20))

    gpd.GeoDataFrame(c_new_edges.copy()).plot(ax=ax, color='gray', alpha=0.2)
    gpd.GeoDataFrame(c_short_path_edges.copy()).plot(ax=ax, zorder=1, linewidth=c_short_path_edges.count_weight, color='blue')
    # gpd.GeoDataFrame(c_short_path_edges.copy()).plot(ax=ax, zorder=1, linewidth=(c_short_path_edges.weights / 50), color='blue')

def plot_chosen_route(dict_fastest):

    fig, ax = plt.subplots(1, 1, figsize=(30, 20))

    gpd.GeoDataFrame(dict_fastest['new_edges'].copy()).plot(ax=ax, color='gray', alpha=0.2)
    gpd.GeoDataFrame(dict_fastest['shortest_path_edges'].copy()).plot(ax=ax, zorder=1, linewidth=(dict_fastest['shortest_path_edges'].count_weight), color='green')
    # gpd.GeoDataFrame(dict_fastest['shortest_path_edges'].copy()).plot(ax=ax, zorder=1, linewidth=(dict_fastest['shortest_path_edges'].weights / 50), color='green')