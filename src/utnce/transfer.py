import geopandas
import geopandas as gpd
import pandas as pd
from osgeo import ogr,gdal
import numpy 
from shapely.wkb import loads
from shapely.geometry import Point
from shapely.geometry import LineString
from geopy.distance import geodesic
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
    
    # Return the modified 'new_nodes' GeoDataFrame
    return new_nodes


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

    # Merge 'edges' with 'shortest_path_edges_df' based on 'geometry' to add additional data to 'edges'
    new_edges = pd.merge(edges, shortest_path_edges_df, on='geometry', how='outer')
    
    # Select and rename relevant columns in 'new_edges'
    new_edges = new_edges[['osm_id_x', 'geometry', 'railway_x', 'service_x', 'id_x', 'from_id_x', 'to_id_x', 'distance_x', 'time_x', 'weights_x', 'to_from_x', 'from_to_x', 'count_weight', 'route_name_list', 'route', 'ref']]
    new_edges = new_edges.rename(columns={'osm_id_x': 'osm_id',
                                         'railway_x': 'railway',
                                         'service_x': 'service',
                                         'id_x': 'id',
                                         'from_id_x': 'from_id',
                                         'to_id_x': 'to_id',
                                         'distance_x': 'distance',
                                         'time_x': 'time',
                                         'weights_x': 'weights',
                                         'to_from_x': 'to_from',
                                         'from_to_x': 'from_to'})
    
    # Return the modified 'new_edges' DataFrame
    return new_edges



def s_e_node_df(s_e_coordinates, new_nodes):
    start_end_points_coordinates_pairs = pd.DataFrame([s_e_coordinates])
    start_end_points_coordinates_pairs = s_e_coordinates_pairs(start_end_points_coordinates_pairs)

    start_end_nearest_id_pairs = id_pairs(start_end_points_coordinates_pairs, new_nodes)
    start_node = new_nodes[new_nodes['id'] == start_end_nearest_id_pairs.loc[0]['s_id']]
    end_node = new_nodes[new_nodes['id'] == start_end_nearest_id_pairs.loc[0]['e_id']]

    return start_node, end_node

def s_e_on_route_ref(start_node, end_node):
    start_node_ref = start_node.ref.to_list()
    s_ref_elements = start_node_ref[0].split(', ')
    s_on_route_ref = pd.DataFrame({ 'ref': s_ref_elements })

    end_node_ref = end_node.ref.to_list()
    e_ref_elements = end_node_ref[0].split(', ')
    e_on_route_ref = pd.DataFrame({ 'ref': e_ref_elements })
    return s_on_route_ref,e_on_route_ref
    
def judge_on_route(s_on_route_ref, e_on_route_ref):   # version4

    length_s_on_route_ref = len(s_on_route_ref)
    length_e_on_route_ref = len(e_on_route_ref)

    if length_s_on_route_ref < 1:
        print("There is something wrong with function----s_e_on_route_gdf")

    
    elif length_s_on_route_ref == 1:
        if length_e_on_route_ref > 1:
            s_e_on_route = pd.concat([s_on_route_ref,e_on_route_ref],ignore_index=True)
            if s_e_on_route.duplicated().sum() == 1:
                print("One of e_on_route is the same as s_on_route \n")
                print("The next step is to find the same route----using function 's_e_same_routes'")
                return s_on_route_ref, e_on_route_ref
            else:
                print("None of e_on_route is the same as s_on_route \n")
                print("The next step is to find transfer stations of e_on_route and s_on_route----using function 'transfer_station_btw'")
                return s_on_route_ref, e_on_route_ref

        elif length_e_on_route_ref == 1:
            if s_on_route_ref.equals(e_on_route_ref) == True:
                print("s_node and e_node are on one same route \n")
                print("The next step is to find the same route----using function 's_e_same_routes'")
                return s_on_route_ref, e_on_route_ref
            else:
                print("s_node and e_node are on two different routes \n")
                print("The next step is to find transfer stations of e_on_route and s_on_route----using function 'transfer_station_btw'")
                return s_on_route_ref, e_on_route_ref

        else:
            print("There is something wrong with function----s_e_on_route_gdf")


    else:
        s_e_on_route = pd.concat([s_on_route_ref,e_on_route_ref],ignore_index=True)
        if length_e_on_route_ref > 1:
            if s_e_on_route.duplicated().sum() == len(s_on_route_ref):
                print("s_node and e_node are on several same routes \n")
                print("The next step is to find the same routes----using function 's_e_same_routes'")
                return s_on_route_ref, e_on_route_ref        
            elif s_e_on_route.duplicated().sum() == 0:
                print("Any of e_on_route is not the same as any of s_on_route \n")
                print("The next step is to find transfer stations of e_on_route and s_on_route----using function 'transfer_station_btw'")
                return s_on_route_ref, e_on_route_ref            
            else:
                s_e_same_route = s_e_on_route[s_e_on_route['ref'].duplicated()]
                print("Some of e_on_route is the same as some of s_on_route \n")
                print("The next step is to find the same routes----using function 's_e_same_routes'")
                return s_on_route_ref, e_on_route_ref            
            
        elif length_e_on_route_ref == 1:
            if s_e_on_route.duplicated().sum() == 1:
                print("One of s_on_route is the same as e_on_route \n")
                print("The next step is to find the same routes----using function 's_e_same_routes'")
                return s_on_route_ref, e_on_route_ref        
            else:
                print("None of s_on_route is the same as e_on_route \n")
                print("The next step is to find transfer stations of e_on_route and s_on_route----using function 'transfer_station_btw'")
                return s_on_route_ref, e_on_route_ref
        
        else:
            print("There is something wrong with function----s_e_on_route_gdf")

            
### The next step is to find the same route----using function 's_e_same_routes'
def s_e_same_routes(s_on_route_ref,e_on_route_ref):
    results = s_on_route_ref['ref'].isin(e_on_route_ref['ref'].tolist())
    s_e_same_routes_df = s_on_route_ref[results]
    if len(s_e_same_routes_df) > 0:
        print(f"s_node and e_node are on one same route:{s_e_same_routes_df}\n")
        return s_e_same_routes_df
    else:
        print("s_node and e_node are on totally different routes\n")
        # print(f"The next step is to find transfer station of {s_on_route_ref.loc[0,'ref']} and {e_on_route_ref.loc[0,'ref']}")
        print(f"The next step is to find transfer station of s_on_route:{s_on_route_ref['ref'].tolist()} and e_on_route:{e_on_route_ref['ref'].tolist()}")
    
        return s_e_same_routes_df


### The next step is to find transfer stations of e_on_route and s_on_route----using function 'transfer_station_btw'



        






### Get all stations between s_e nodes or s_transfer nodes or transfer_e nodes
def all_stations_on_matched_route(order_route_dict, node_on_route_gdf):
    node_matched_all_stations_dict = {}

    for key,df in order_route_dict.items():
        matched_rows = node_on_route_gdf[node_on_route_gdf['name'] == key]
        if not matched_rows.empty:
            node_matched_all_stations_dict[key] = df
    
    return node_matched_all_stations_dict

def all_stations_on_matched_routes(s_e_same_routes_df,sub_routes,start_node,end_node,sub_order_route_dict):
    s_e_same_route_df_new = pd.merge(s_e_same_routes_df,sub_routes,on='ref',how='left')
    
    sub_routes_gdf = gpd.GeoDataFrame(sub_routes.copy())
    s_e_node_gdf = gpd.GeoDataFrame(pd.concat([start_node.copy(), end_node.copy()],ignore_index=True))
    e_node_on_route_gdf = sub_routes_gdf[sub_routes_gdf.geometry.intersects(s_e_node_gdf.iloc[1].geometry)]

    s_e_same_route_gdf = pd.merge(s_e_same_route_df_new,e_node_on_route_gdf,how='inner')
    matched_route_all_stations_dict = all_stations_on_matched_route(sub_order_route_dict, s_e_same_route_gdf)
    all_stations_on_matched_routes_dfs= list(matched_route_all_stations_dict.values())
    return all_stations_on_matched_routes_dfs

def find_nearest_station(coordinate, nodes):
    node_tree = shapely.STRtree(nodes.geometry)
    find_nearest = node_tree.nearest(shapely.points(coordinate))
    return nodes.iloc[[find_nearest]]

def between_stations_one_way(start_node,end_node,one_matched_route_df):
    start_node_coordinate = start_node.iloc[0]['coordinate_value']
    s_station_df1 = find_nearest_station(start_node_coordinate, one_matched_route_df)
    s_index_df1 = s_station_df1.index[0]

    end_node_coordinate = end_node.iloc[0]['coordinate_value']
    e_station_df1 = find_nearest_station(end_node_coordinate, one_matched_route_df)
    e_index_df1 = e_station_df1.index[0]

    if s_index_df1 > e_index_df1:
        s_e_between_stations = one_matched_route_df.iloc[s_index_df1:e_index_df1-1:-1]
    else:
        s_e_between_stations = one_matched_route_df.iloc[s_index_df1:e_index_df1+1:1]
    return s_e_between_stations

def btw_stations_each_way_list(start_node,end_node,all_stations_on_matched_routes_dfs):
    btw_stations_each_way_list = []
    for i in range(len(all_stations_on_matched_routes_dfs)):
        one_matched_route_df = all_stations_on_matched_routes_dfs[i]
        df1 = between_stations_one_way(start_node,end_node,one_matched_route_df)
        btw_stations_each_way_list.append(df1)
    return btw_stations_each_way_list


    
### Get all id_pairs of stations between s_e nodes or s_transfer nodes or transfer_e nodes
def btw_all_ids_pairs(btw_stations_each_way_list,sub_new_nodes):
    btw_all_id_pairs_list = []
    for i in range(len(btw_stations_each_way_list)):
        s_e_between_nodes_way1 = sub_new_nodes.merge(btw_stations_each_way_list[i], on='coordinate_value', suffixes=('', '_drop'))
        id_pairs_way1 = pd.DataFrame(columns = ['s_id','e_id'])

        for i in range(len(s_e_between_nodes_way1)-1):
            id_pairs_way1.loc[i,'s_id'] = s_e_between_nodes_way1.loc[i,'id']
            id_pairs_way1.loc[i,'e_id'] = s_e_between_nodes_way1.loc[i+1,'id']
        
        btw_all_id_pairs_list.append(id_pairs_way1)    
    return btw_all_id_pairs_list


### Drop duplicate dataframe for id_pairs of stations between s_e nodes or s_transfer nodes or transfer_e nodes
def hash_dataframe(df):
    return hash(tuple(df.values.ravel()))

def drop_duplicate_dataframes(df_list):
    # 使用哈希值去除重复的DataFrame
    unique_dfs = {hash_dataframe(df): df for df in df_list}.values()

    return list(unique_dfs)

def drop_df_in_list(df_list):
    unique_dfs = drop_duplicate_dataframes(df_list)
    btw_all_id_pairs_list_unique = list(unique_dfs)
    if len(unique_dfs) > 1:
        # btw_all_id_pairs_list_unique = list(unique_dfs)
        print("At least one dataframe in the list(btw_all_id_pairs_list) is different")
        return btw_all_id_pairs_list_unique
    else:
        print("All dataframes in the list are the same")
        return btw_all_id_pairs_list_unique




        
###### Judgement function
######ideas from elco-- judgement    
# def route_decision():
    
#     route_list = {51: station_df,
#                   52: station_df,
#                   53: station_df,
#                   54: station_df,
#             }

#     start_node = (5.0902342,52.2314)
#     end_node = (5.42902342,52.4314)


#     # catch the nearest metro station to start and end node on the same route
#     collect_stations_all = {}

#     for route in route_list:
        
#         collect_stations = []
#         # find nearest station to start and end node
#         if route_list[route].distance(start_node).min() < 500: #meters
#             metro_station_start = route_list[route].distance(start_node).idxmin()
#             collect_stations.append(metro_station_start)
        
#         elif route_list[route].distance(end_node).min() < 500: #meters
#             metro_station_end = route_list[route].distance(end_node).idxmin()
#             collect_stations.append(metro_station_end)

#         collect_stations_all[route] = collect_stations

#     # catch the nearest metro station to start and end node on a different route

#     transfer_dataframe



# def s_e_node_df(s_e_coordinates, new_nodes):
#     start_end_points_coordinates_pairs = pd.DataFrame([s_e_coordinates])
#     start_end_points_coordinates_pairs = s_e_coordinates_pairs(start_end_points_coordinates_pairs)

#     start_end_nearest_id_pairs = id_pairs(start_end_points_coordinates_pairs, new_nodes)
#     start_node = new_nodes[new_nodes['id'] == start_end_nearest_id_pairs.loc[0]['s_id']]
#     end_node = new_nodes[new_nodes['id'] == start_end_nearest_id_pairs.loc[0]['e_id']]

#     return start_node, end_node


# def s_e_on_route_gdf(start_node, end_node, routes_df):
#     routes_gdf = gpd.GeoDataFrame(routes_df.copy())
#     s_e_node_gdf = gpd.GeoDataFrame(pd.concat([start_node.copy(), end_node.copy()],ignore_index=True))

#     s_node_on_route_gdf = routes_gdf[routes_gdf.geometry.intersects(s_e_node_gdf.iloc[0].geometry)]
#     e_node_on_route_gdf = routes_gdf[routes_gdf.geometry.intersects(s_e_node_gdf.iloc[1].geometry)]
    
#     return s_node_on_route_gdf, e_node_on_route_gdf

###### version3
# def judge_on_route(s_node_on_route_gdf, e_node_on_route_gdf):   
#     s_on_route_ref = s_node_on_route_gdf[['ref']].reset_index(drop=True)
#     e_on_route_ref = e_node_on_route_gdf[['ref']].reset_index(drop=True)

#     length_s_on_route_ref = len(s_on_route_ref)
#     length_e_on_route_ref = len(e_on_route_ref)

#     if length_s_on_route_ref < 1:
#         print("There is something wrong with function----s_e_on_route_gdf")

    
#     elif length_s_on_route_ref == 1:
#         if length_e_on_route_ref > 1:
#             s_e_on_route = pd.concat([s_on_route_ref,e_on_route_ref],ignore_index=True)
#             if s_e_on_route.duplicated().sum() == 1:
#                 print(f"One of e_on_route is the same as s_on_route:{s_on_route_ref.loc[0,'ref']}\n")
#                 print("The next step is to find transfer stations of the rest of e_on_route and s_on_route, then find all stations between start_node/tansfer stations/end_node, finally calculate shortest path----using function ''")
#                 return s_on_route_ref, e_on_route_ref
#             else:
#                 print(f"None of e_on_route is the same as s_on_route:{s_on_route_ref.loc[0,'ref']}\n")
#                 print("The next step is to find transfer stations of all e_on_route and s_on_route, then find all stations between start_node/tansfer stations/end_node, finally calculate shortest path----using function ''")
#                 return s_on_route_ref, e_on_route_ref

#         elif length_e_on_route_ref == 1:
#             if s_on_route_ref.equals(e_on_route_ref) == True:
#                 print(f"s_node and e_node are on one same route:{s_on_route_ref.loc[0,'ref']}\n")
#                 print("The next step is to find all stations between start_node and end_node, then calculate shortest path----using function '' ")
#                 return s_on_route_ref, e_on_route_ref
#             else:
#                 print(f"s_node and e_node are on two different routes:{s_on_route_ref.loc[0,'ref']},{e_on_route_ref.loc[0,'ref']}")
#                 print(f"The next step is to find transfer station of {s_on_route_ref.loc[0,'ref']} and {e_on_route_ref.loc[0,'ref']}, then find all stations between start_node and end_node, finally calculate shortest path----using function '' ")
#                 return s_on_route_ref, e_on_route_ref

#         else:
#             print("There is something wrong with function----s_e_on_route_gdf")


#     else:
#         s_e_on_route = pd.concat([s_on_route_ref,e_on_route_ref],ignore_index=True)
#         if length_e_on_route_ref > 1:
#             if s_e_on_route.duplicated().sum() == len(s_on_route_ref):
#                 print(f"s_node and e_node are on several same routes:{s_on_route_ref.ref.tolist()}\n")
#                 print("The next step is to find all stations between start_node and end_node on each route,then calculate shortest path----using function '' ")
#                 return s_on_route_ref, e_on_route_ref        
#             elif s_e_on_route.duplicated().sum() == 0:
#                 print("Any of e_on_route is not the same as any of s_on_route")
#                 print("The next step is to enumerate different transfer routes through permutation and combination, obtain all transfer stations, then find all stations between the start node/transfer stations/end node, and finally calculate the shortest path----using function '' ")
#                 return s_on_route_ref, e_on_route_ref            
#             else:
#                 s_e_same_route = s_e_on_route[s_e_on_route['ref'].duplicated()]
#                 print(f"Some of e_on_route is the same as some of s_on_route:{s_e_same_route.ref.tolist()}\n")
#                 print("The next step is to enumerate different transfer routes through permutation and combination, obtain all transfer stations, then find all stations between the start node/transfer stations/end node, and finally calculate the shortest path----using function '' ")
#                 return s_on_route_ref, e_on_route_ref            
            
#         elif length_e_on_route_ref == 1:
#             if s_e_on_route.duplicated().sum() == 1:
#                 print(f"One of s_on_route is the same as e_on_route:{e_on_route_ref.loc[0,'ref']}\n")
#                 print("The next step is to find transfer stations of the rest of s_on_route and e_on_route, then find all stations between start_node, tansfer stations and end_node, finally calculate shortest path----using function ''")
#                 return s_on_route_ref, e_on_route_ref        
#             else:
#                 print(f"None of s_on_route is the same as e_on_route:{e_on_route_ref.loc[0,'ref']}\n")
#                 print("The next step is to find transfer stations of all s_on_route and e_on_route, then find all stations between start_node/tansfer stations/end_node, finally calculate shortest path----using function ''")
#                 return s_on_route_ref, e_on_route_ref
        
#         else:
#             print("There is something wrong with function----s_e_on_route_gdf")

###### version2-3
# def judge_on_route(s_node_on_route_gdf, e_node_on_route_gdf):    
#     s_node_on_route_gdf_reset = s_node_on_route_gdf['ref'].reset_index(drop=True)
#     e_node_on_route_gdf_reset = e_node_on_route_gdf['ref'].reset_index(drop=True)

#     if s_node_on_route_gdf_reset.equals(e_node_on_route_gdf_reset) == True:
#         s_e_nodes_on_route_gdf = s_node_on_route_gdf
#         length_of_on_route_gdf = len(s_e_nodes_on_route_gdf)
#         if length_of_on_route_gdf == 1:
#             print(f"s_node and e_node are on the same one route")
#             return s_e_nodes_on_route_gdf
#         else:
#             print(f"s_node and e_node are on {length_of_on_route_gdf} same routes")
#             return s_e_nodes_on_route_gdf
      
#     else:
#         num_rows_s = len(s_node_on_route_gdf)
#         s_node_on_route_gdf['s_or_e'] = pd.Series(['s'] * num_rows_s, name='s_or_e', index=s_node_on_route_gdf.index)
       
#         num_rows_e = len(e_node_on_route_gdf)
#         e_node_on_route_gdf['s_or_e'] = pd.Series(['e'] * num_rows_e, name='s_or_e', index=e_node_on_route_gdf.index)
    
#         s_e_nodes_on_route_gdf = pd.concat([s_node_on_route_gdf, e_node_on_route_gdf], ignore_index=True)
        
#         print(f"s_node and e_node are on different routes")
#         return s_e_nodes_on_route_gdf

###### version2    
# def judge_on_route(s_node_on_route_gdf, e_node_on_route_gdf):    
#     s_node_on_route_gdf_reset = s_node_on_route_gdf['ref'].reset_index(drop=True)
#     e_node_on_route_gdf_reset = e_node_on_route_gdf['ref'].reset_index(drop=True)

#     if s_node_on_route_gdf_reset.equals(e_node_on_route_gdf_reset) == True:
#         s_e_nodes_on_route_gdf = s_node_on_route_gdf
#         length_of_on_route_gdf = len(s_e_nodes_on_route_gdf)
#         if length_of_on_route_gdf == 1:
#             print(f"s_node and e_node are on the same one route")
#             return s_e_nodes_on_route_gdf
#         else:
#             print(f"s_node and e_node are on {length_of_on_route_gdf} same routes")
#             return s_e_nodes_on_route_gdf
#     else:
#         num_rows_s = len(s_node_on_route_gdf)
#         s_node_on_route_gdf['s_or_e'] = pd.Series(['s'] * num_rows_s, name='s_or_e', index=s_node_on_route_gdf.index)
       
#         num_rows_e = len(e_node_on_route_gdf)
#         e_node_on_route_gdf['s_or_e'] = pd.Series(['e'] * num_rows_e, name='s_or_e', index=e_node_on_route_gdf.index)
    
#         s_e_nodes_on_route_gdf = pd.concat([s_node_on_route_gdf, e_node_on_route_gdf], ignore_index=True)
        
#         print(f"s_node and e_node are on different routes")
#         return s_e_nodes_on_route_gdf

###### version1    
# def judge_on_route(s_node_on_route_gdf, e_node_on_route_gdf):   
#     if s_node_on_route_gdf.equals(e_node_on_route_gdf) == True:
#         s_e_nodes_on_route_gdf = s_node_on_route_gdf
#         length_of_on_route_gdf = len(s_e_nodes_on_route_gdf)
#         if length_of_on_route_gdf == 1:
#             print(f"s_node and e_node are on the same one route")
#             return s_e_nodes_on_route_gdf
#         else:
#             print(f"s_node and e_node are on {length_of_on_route_gdf} same routes")
#             return s_e_nodes_on_route_gdf
#     else:
#         num_rows_s = len(s_node_on_route_gdf)
#         s_node_on_route_gdf['s_or_e'] = pd.Series(['s'] * num_rows_s, name='s_or_e', index=s_node_on_route_gdf.index)
       
#         num_rows_e = len(e_node_on_route_gdf)
#         e_node_on_route_gdf['s_or_e'] = pd.Series(['e'] * num_rows_e, name='s_or_e', index=e_node_on_route_gdf.index)
    
#         s_e_nodes_on_route_gdf = pd.concat([s_node_on_route_gdf, e_node_on_route_gdf], ignore_index=True)
        
#         print(f"s_node and e_node are on different routes")
#         return s_e_nodes_on_route_gdf

























# def transfer_shortest_path(s_e_coordinates, new_edges, new_nodes):
    
#     start_end_points_coordinates_pairs = pd.DataFrame([s_e_coordinates])
#     start_end_points_coordinates_pairs = s_e_coordinates_pairs(start_end_points_coordinates_pairs)
#     start_end_nearest_id_pairs = id_pairs(start_end_points_coordinates_pairs, new_nodes)
#     start_point_id = start_end_nearest_id_pairs.iloc[0]['s_id']
#     end_point_id = start_end_nearest_id_pairs.iloc[0]['e_id']
    
#     G = create_ground_graph(new_edges, new_nodes)
#     path_s_e, length_s_e, short_path_edges = shortest_path(G, start_point_id, end_point_id, new_edges, weight = "weight")

#     return path_s_e, length_s_e, short_path_edges,start_end_points_coordinates_pairs,start_end_nearest_id_pairs

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