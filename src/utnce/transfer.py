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
    
    new_nodes_gdf = gpd.GeoDataFrame(new_nodes.copy())
    new_nodes_gdf['geo_x'] = new_nodes_gdf.geometry.x
    new_nodes_gdf['geo_y'] = new_nodes_gdf.geometry.y
    new_nodes_gdf['coordinate_value'] = list(zip(new_nodes_gdf['geo_x'], new_nodes_gdf['geo_y']))
    new_nodes = new_nodes_gdf
    
    return new_nodes

def add_columns_to_edges(order_route_dict, aggregation_functions, edges):
    
    order_route_stations_df = pd.concat(order_route_dict.values()).reset_index(drop=True)
    new_order_route_stations_df = order_route_stations_df.groupby('id').agg(aggregation_functions).reset_index()
    new_edges = pd.merge(edges, new_order_route_stations_df, on='geometry', how='outer')

    new_edges = new_edges.drop(columns=['id_y'])
    new_edges = new_edges.rename(columns={'id_x': 'id'})

    return new_edges






def s_e_node_df(s_e_coordinates, new_nodes):
    start_end_points_coordinates_pairs = pd.DataFrame([s_e_coordinates])
    start_end_points_coordinates_pairs = s_e_coordinates_pairs(start_end_points_coordinates_pairs)

    start_end_nearest_id_pairs = id_pairs(start_end_points_coordinates_pairs, new_nodes)
    start_node = new_nodes[new_nodes['id'] == start_end_nearest_id_pairs.loc[0]['s_id']]
    end_node = new_nodes[new_nodes['id'] == start_end_nearest_id_pairs.loc[0]['e_id']]

    return start_node, end_node


def s_e_on_route_gdf(start_node, end_node, routes_df):
    routes_gdf = gpd.GeoDataFrame(routes_df.copy())
    s_e_node_gdf = gpd.GeoDataFrame(pd.concat([start_node.copy(), end_node.copy()],ignore_index=True))

    s_node_on_route_gdf = routes_gdf[routes_gdf.geometry.intersects(s_e_node_gdf.iloc[0].geometry)]
    e_node_on_route_gdf = routes_gdf[routes_gdf.geometry.intersects(s_e_node_gdf.iloc[1].geometry)]
    
    return s_node_on_route_gdf, e_node_on_route_gdf


def judge_on_route(s_node_on_route_gdf, e_node_on_route_gdf):
    if s_node_on_route_gdf.equals(e_node_on_route_gdf) == True:
        s_e_nodes_on_route_gdf = s_node_on_route_gdf
        length_of_on_route_gdf = len(s_e_nodes_on_route_gdf)
        if length_of_on_route_gdf == 1:
            print(f"s_node and e_node are on the same one route")
            return s_e_nodes_on_route_gdf
        else:
            print(f"s_node and e_node are on {length_of_on_route_gdf} same routes")
            return s_e_nodes_on_route_gdf
    else:
        num_rows_s = len(s_node_on_route_gdf)
        s_node_on_route_gdf['s_or_e'] = pd.Series(['s'] * num_rows_s, name='s_or_e', index=s_node_on_route_gdf.index)
       
        num_rows_e = len(e_node_on_route_gdf)
        e_node_on_route_gdf['s_or_e'] = pd.Series(['e'] * num_rows_e, name='s_or_e', index=e_node_on_route_gdf.index)
    
        s_e_nodes_on_route_gdf = pd.concat([s_node_on_route_gdf, e_node_on_route_gdf], ignore_index=True)
        
        print(f"s_node and e_node are on different routes")
        return s_e_nodes_on_route_gdf


def all_stations_on_matched_route(order_route_dict, node_on_route_gdf):
    node_matched_all_stations_dict = {}

    for key,df in order_route_dict.items():
        matched_rows = node_on_route_gdf[node_on_route_gdf['name'] == key]
        if not matched_rows.empty:
            node_matched_all_stations_dict[key] = df
    
    return node_matched_all_stations_dict


def find_nearest_station(coordinate, nodes):
    node_tree = shapely.STRtree(nodes.geometry)
    find_nearest = node_tree.nearest(shapely.points(coordinate))
    return nodes.iloc[[find_nearest]]






















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