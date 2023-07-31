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


def add_ref_to_orderroutes_or_shortestpath_dict(order_route_dict, routes_df):
       
    route_name_list = list(order_route_dict.keys())
    for key, df in order_route_dict.items():
        df['route_name_list'] = key

    for line in order_route_dict.keys():
        order_route_list = order_route_dict[line]
        for i in range(len(order_route_list)):
            for j in range(len(routes_df)):
                if order_route_list.iloc[i]['route_name_list'] == routes_df.iloc[j]['name']:
                    order_route_list.at[i, 'ref'] = routes_df.iloc[j]['ref']
                    order_route_list.at[i, 'route'] = routes_df.iloc[j]['route']
                    break 
        order_route_dict[line] = order_route_list
        
    return order_route_dict

def create_tuple_column(df):
    
    df['coordinate_value'] = list(zip(df['geo_x'], df['geo_y']))
    
    return df

def check_transfer(transfer):
    
    if isinstance(transfer, tuple) and len(transfer) > 1 and len(set(transfer)) > 1:
        
        return transfer

def check_ref(ref):
    
    if isinstance(ref, str) and len(ref) > 1 and not all(item is None for item in ref):
        ref = ref.replace(" ", "").replace(",", "")
        
        return tuple(ref)

def add_columns_to_nodes(order_route_dict, aggregation_functions, nodes):
        
    for key, df in order_route_dict.items():
        new_df = create_tuple_column(df)
        order_route_dict[key] = new_df
        
    order_route_stations_df = pd.concat(order_route_dict.values()).reset_index(drop=True)
    new_order_route_stations_df = order_route_stations_df.groupby('coordinate_value').agg(aggregation_functions).reset_index()
    new_nodes = pd.merge(nodes, new_order_route_stations_df, on='geometry', how='outer')
    new_nodes['transfer'] = new_nodes['ref'].apply(check_ref)
    new_nodes['transfer'] = new_nodes['transfer'].apply(check_transfer)

    return new_nodes

def add_columns_to_edges(order_route_dict, aggregation_functions, edges):
    
    order_route_stations_df = pd.concat(order_route_dict.values()).reset_index(drop=True)
    new_order_route_stations_df = order_route_stations_df.groupby('id').agg(aggregation_functions).reset_index()
    new_edges = pd.merge(edges, new_order_route_stations_df, on='geometry', how='outer')

    return new_edges

def transfer_shortest_path(s_e_coordinates, new_edges, new_nodes):
    
    start_end_points_coordinates_pairs = pd.DataFrame([s_e_coordinates])
    start_end_points_coordinates_pairs = s_e_coordinates_pairs(start_end_points_coordinates_pairs)
    start_end_nearest_id_pairs = id_pairs(start_end_points_coordinates_pairs, new_nodes)
    start_point_id = start_end_nearest_id_pairs.iloc[0]['s_id']
    end_point_id = start_end_nearest_id_pairs.iloc[0]['e_id']
    
    G = create_ground_graph(new_edges, new_nodes)
    path_s_e, length_s_e, short_path_edges = shortest_path(G, start_point_id, end_point_id, new_edges, weight = "weight")

    return path_s_e, length_s_e, short_path_edges,start_end_points_coordinates_pairs,start_end_nearest_id_pairs

def walking_linear_distance(start_end_points_coordinates_pairs,start_end_nearest_id_pairs,new_nodes):
    
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
    
    return using_time_hour,using_time_minutes

def plot_chosen_route(new_edges,short_path_edges):

    fig, ax = plt.subplots(1, 1, figsize=(30, 20))

    gpd.GeoDataFrame(new_edges.copy()).plot(ax=ax, color='gray', alpha=0.2)
    gpd.GeoDataFrame(short_path_edges.copy()).plot(ax=ax, zorder=1, linewidth=(short_path_edges.count_weight) * 2, color='orange')