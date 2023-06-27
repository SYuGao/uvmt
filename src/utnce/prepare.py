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

import warnings
warnings.filterwarnings("ignore")

gdal.SetConfigOption("OSM_CONFIG_FILE", "osmconf.ini")

# Define a helper function to generate pairs of consecutive elements in a list
def pairwise(iterable):
    "s -> (s0, s1), (s1, s2), (s2, s3), ..."
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)
    
# Define a helper function to generate permutations
def permutations(iterable, r=None):
   # permutations('ABCD', 2) --> AB AC AD BA BC BD CA CB CD DA DB DC
    # permutations(range(3)) --> 012 021 102 120 201 210
    pool = tuple(iterable)
    n = len(pool)
    r = n if r is None else r
    if r > n:
        return
    indices = list(range(n))
    cycles = list(range(n, n-r, -1))
    yield tuple(pool[i] for i in indices[:r])
    while n:
        for i in reversed(range(r)):
            cycles[i] -= 1
            if cycles[i] == 0:
                indices[i:] = indices[i+1:] + indices[i:i+1]
                cycles[i] = n - i
            else:
                j = cycles[i]
                indices[i], indices[-j] = indices[-j], indices[i]
                yield tuple(pool[i] for i in indices[:r])
                break
        else:
            return 
        
# Retrive data from OSM and get the geographic data of tram
def query_b(geoType,keyCol,**valConstraint):
    """
    This function builds an SQL query from the values passed to the retrieve() function.
    Arguments:
         *geoType* : Type of geometry (osm layer) to search for.
         *keyCol* : A list of keys/columns that should be selected from the layer.
         ***valConstraint* : A dictionary of constraints for the values. e.g. WHERE 'value'>20 or 'value'='constraint'
    Returns:
        *string: : a SQL query string.
    """
    query = "SELECT " + "osm_id"
    for a in keyCol: query+= ","+ a  
    query += " FROM " + geoType + " WHERE "
    # If there are values in the dictionary, add constraint clauses
    if valConstraint: 
        for a in [*valConstraint]:
            # For each value of the key, add the constraint
            for b in valConstraint[a]: query += a + b
        query+= " AND "
    # Always ensures the first key/col provided is not Null.
    query+= ""+str(keyCol[0]) +" IS NOT NULL" 
    return query 

def retrieve(osm_path,geoType,keyCol,**valConstraint):
    """
    Function to extract specified geometry and keys/values from OpenStreetMap
    Arguments:
        *osm_path* : file path to the .osm.pbf file of the region 
        for which we want to do the analysis.     
        *geoType* : Type of Geometry to retrieve. e.g. lines, multipolygons, etc.
        *keyCol* : These keys will be returned as columns in the dataframe.
        ***valConstraint: A dictionary specifiying the value constraints.  
        A key can have multiple values (as a list) for more than one constraint for key/value.  
    Returns:
        *GeoDataFrame* : a geopandas GeoDataFrame with all columns, geometries, and constraints specified.    
    """
    driver=ogr.GetDriverByName('OSM')
    data = driver.Open(osm_path)
    query = query_b(geoType,keyCol,**valConstraint)
    sql_lyr = data.ExecuteSQL(query)
    features =[]
    # cl = columns 
    cl = ['osm_id']
    
    
    for a in keyCol: cl.append(a)
    if data is not None:
        for feature in sql_lyr:
            try:
                if feature.GetField(keyCol[0]) is not None:
                    shapely_geo = shapely.from_wkt(feature.geometry().ExportToWkt())
                    if shapely_geo is None:
                        continue
                    # field will become a row in the dataframe.
                    field = []
                    for i in cl: field.append(feature.GetField(i))
                    field.append(shapely_geo)   
                    features.append(field)
            except:
                print("WARNING: skipped OSM feature")
      
    cl.append('geometry')                   
    if len(features) > 0:
        return geopandas.GeoDataFrame(features,columns=cl) #,crs={'init': 'epsg:4326'}
    else:
        print("WARNING: No features or No Memory. returning empty GeoDataFrame") 
        return geopandas.GeoDataFrame(columns=['osm_id','geometry']) #,crs={'init': 'epsg:4326'}    

    
def railway(osm_path):
    """
    Function to extract railway linestrings from OpenStreetMap
    Arguments:
        *osm_path* : file path to the .osm.pbf file of the region for which we want to do the analysis.       
    Returns:
        *GeoDataFrame* : a geopandas GeoDataFrame with all railway linestrings.
    """ 
    return retrieve(osm_path,'lines',['railway','service'])        

def subway_network(osm_path):
    """
    Extract subway network information from an OpenStreetMap XML file.

    Args:
    - osm_path: a string representing the path to the OpenStreetMap XML file

    Returns:
    - a pandas DataFrame containing information about subway railways in the OpenStreetMap data

    Example:
    >>> import pandas as pd
    >>> osm_path = 'path/to/osm/file.xml'
    >>> subway_network = subway_network(osm_path)
    >>> print(subway_network.head())
          osmid        name        railway  ...    ref tunnel        geometry
    1234  1234    Line 1    subway   ...  M1    yes  LINESTRING (0.0 0.0, 1.0 1.0)
    5678  5678    Line 2    subway   ...  M2    yes  LINESTRING (1.0 1.0, 2.0 2.0)

    Note: This function requires the osmnx library to be installed and OpenStreetMap data with railway information to be available.
    """
    # Use the osmnx library to extract railway information from the OpenStreetMap data
    df_railway = railway(osm_path)
    
    # Filter the DataFrame to include only subway railways
    subway = df_railway.loc[df_railway.railway == 'subway']
    
    return subway

def tram_network(osm_path):
    """
    Extracts tram network data from an OpenStreetMap file at the specified file path and returns it as a Pandas DataFrame.

    Parameters:
    osm_path (str): The file path of the OpenStreetMap file.

    Returns:
    Pandas DataFrame: A DataFrame containing tram network data extracted from the OpenStreetMap file.
    """

    # Extract railway data from the OpenStreetMap file
    df_railway = railway(osm_path)
    
    # Filter the DataFrame to only contain tram network data
    tram = df_railway.loc[df_railway.railway == 'tram']
    
    # Return the filtered DataFrame
    return tram

def road(osm_path):
    
    return retrieve(osm_path,'lines',['highway'])


def public_stations(osm_path):
    
    return (retrieve(osm_path,'points',['public_transport', 'railway', 'tram', 'subway', 'highway', 'bus', 'name']))

def sub_stations(osm_path):
    """
    Extracts and filters subway stations from OpenStreetMap data.
    
    Parameters:
        osm_path (str): The path to the OpenStreetMap data.
        
    Returns:
        pandas.DataFrame: A DataFrame containing filtered subway stations with their x and y coordinates.
    """
    
    # Calling the function railway_stations_subway to get a DataFrame of all railway stations
    df_railway_stations = public_stations(osm_path)


    # Filtering the DataFrame to select only subway stations
    sub_stations1 = df_railway_stations.loc[df_railway_stations.subway == 'yes']
    
    # Further filtering to include only stop positions (public_transport == 'stop_position')
    sub_stations1 = sub_stations1.loc[sub_stations1.public_transport == 'stop_position']
    
    sub_stations2 = df_railway_stations.loc[df_railway_stations.subway == 'yes']
    sub_stations2 = sub_stations2.loc[sub_stations2.railway == 'stop'] 
    
    sub_stations = pd.concat([sub_stations1, sub_stations2])
    # Adding new columns 'geo_x' and 'geo_y' to store the x and y coordinates of the stations' geometry
    sub_stations['geo_x'] = sub_stations.geometry.x
    sub_stations['geo_y'] = sub_stations.geometry.y
    
    # Returning the DataFrame of subway stations
    return sub_stations

def tram_stations(osm_path):
   
    df_railway_stations = public_stations(osm_path)

    tram_stations1 = df_railway_stations.loc[df_railway_stations.tram == 'yes']
    tram_stations1 = tram_stations1.loc[tram_stations1.public_transport == 'stop_position']
    
    tram_stations2 = df_railway_stations.loc[df_railway_stations.tram == 'yes']
    tram_stations2 = tram_stations2.loc[tram_stations2.railway == 'tram_stop'] 
    
    tram_stations = pd.concat([tram_stations1, tram_stations2])

    tram_stations['geo_x'] = tram_stations.geometry.x
    tram_stations['geo_y'] = tram_stations.geometry.y
    
    return tram_stations
    
def bus_stations(osm_path):
    
    df_publich__transport_stations = public_stations(osm_path)
    
    bus_stations1 = df_publich__transport_stations.loc[df_publich__transport_stations.highway == 'bus_stop']
    
    bus_stations2 = df_publich__transport_stations.loc[df_publich__transport_stations.bus == 'yes']
    bus_stations2 = bus_stations2.loc[bus_stations2.public_transport == 'stop_position']
    
    bus_stations = pd.concat([bus_stations1, bus_stations2])
    
    bus_stations['geo_x'] = bus_stations.geometry.x
    bus_stations['geo_y'] = bus_stations.geometry.y   
        
    return bus_stations
    
def add_stations(net,station_file):
    
    station_geometries = pd.DataFrame(station_file.geometry)
    
    net.nodes = pd.concat([net.nodes,station_geometries]).reset_index(drop=True)
                           
    return net

# Pre-processing the geographic data of the subway network to obtain 'edges' and 'nodes'
def prepare_network(subway,station_file):
    """
    Prepare a subway network represented as a GeoDataFrame of LineString objects for routing.

    Args:
    - subway: a GeoDataFrame representing the subway network, with 'geometry' column containing LineString objects
    
    Returns:
    - a tuple of two DataFrames representing the edges and nodes of the prepared network

    Example:
    >>> import geopandas
    >>> subway = geopandas.read_file('subway.shp')
    >>> edges, nodes = prepare_network(subway)

    Note: This function requires the shapely, NetworkX, and Pandas libraries to be installed.
    """

    # Build a Network object from the subway edges
    net = Network(edges=subway)

    # Add endpoints to the network where edges don't intersect
    net = add_endpoints(net)
    
    # Add endpoints to the network where edges don't intersect
    net = add_stations(net,station_file)
        
    # Split edges at new endpoints
    net = split_edges_at_nodes(net)

    # Add new endpoints where edges were split
    net = add_endpoints(net)

    # Assign unique IDs to nodes and edges
    net = add_ids(net)

    # Add missing topology information to the network's edges
    net = add_topology(net)    

    # Calculate the degree of each node in the network
    net.nodes['degree'] = calculate_degree(net)

    # Merge edges with a degree of 2
    #net = merge_edges(net)

    # Drop duplicate edges
    net.edges = drop_duplicate_geometries(net.edges, keep='first') 

    # Reset node and edge IDs after fixing topology and merging edges
    net = reset_ids(net) 

    # Add edge distances
    net = add_distances(net)

    # Merge any MultiLineString edges
    net = merge_multilinestrings(net)

    # Add travel time for each edge based on distance and average speed
    net = add_travel_time(net)
    
    # Return the edges and nodes of the prepared network
    return net.edges, net.nodes

def expand_edges(edges):
    """
    Expand a DataFrame of edges into a format that can be used with network analysis algorithms.

    Args:
    - edges: a DataFrame containing edges with columns 'from_id', 'to_id', and 'distance'

    Returns:
    - a DataFrame containing expanded edges with columns 'from_id', 'to_id', 'distance', 'weights', 'to_from', and 'from_to'

    Example:
    >>> edges = pd.DataFrame({'from_id': [0, 0, 1], 'to_id': [1, 2, 2], 'distance': [1.5, 3.2, 2.8]})
    >>> expand_edges(edges)
      from_id  to_id  distance  weights  to_from  from_to
    0       0      1       1.5        1  (0, 1)  (1, 0)
    1       0      2       3.2        3  (0, 2)  (2, 0)
    2       1      2       2.8        2  (1, 2)  (2, 1)

    Notes:
    - The 'weights' column is created by rounding the 'distance' column to the nearest integer.
    - The 'to_from' and 'from_to' columns are created to facilitate conversion between edge formats.
    """
    # Round the distance to the nearest integer and store it in a new column 'weights'
    edges['weights'] = edges['distance'].astype(int)

    # Create 'to_from' and 'from_to' columns to facilitate conversion between edge formats
    edges['to_from'] = list(zip(edges.from_id, edges.to_id))
    edges['from_to'] = list(zip(edges.to_id, edges.from_id))

    # Return the expanded edges DataFrame
    return edges

def routes(osm_path):
    """
    Extracts route data from an OpenStreetMap file at the specified file path and returns it as a Pandas DataFrame.

    Parameters:
    osm_path (str): The file path of the OpenStreetMap file.

    Returns:
    Pandas DataFrame: A DataFrame containing route data extracted from the OpenStreetMap file.
    """

    # Call the 'retrieve' function to extract route data from the OpenStreetMap file
    # The 'retrieve' function takes three arguments:
    # - The file path of the OpenStreetMap file
    # - A list of geometry types to extract (in this case, only multilinestrings)
    # - A list of tag keys to extract (in this case, 'route', 'name', and 'ref')
    route_data = retrieve(osm_path, 'multilinestrings', ['route', 'to', 'name', 'ref'])#,'from'])

    # Return the extracted route data as a Pandas DataFrame
    return route_data

def sub_routes(osm_path):
    df_routes = routes(osm_path)
    sub_routes = pd.DataFrame(df_routes.loc[df_routes.route == 'subway'])
    return sub_routes

def tram_routes(osm_path):
    df_routes = routes(osm_path)
    tram_routes = pd.DataFrame(df_routes.loc[df_routes.route == 'tram'])
    return tram_routes

def sorted_routes(routes_file):     
    routes_file['ref'] = routes_file['ref'].astype(int)
    routes_file = routes_file.sort_values('ref')
    routes_file = routes_file.reset_index(drop=True)
    return routes_file

def check_to_column(sorted_routes_file, all_stations_file):
    
    def check_similarity(row):
        if row['to'] in all_stations_file['name'].values:
            return 'no need to revise'
        else:
            return row
    result = sorted_routes_file.apply(check_similarity, axis=1)
    return print(result)

def start_station_dict(routes_file):
    start_station_name_dict = {}
    for index, row in routes_file.iterrows():
        key = row['name']
        value = row['to']
        start_station_name_dict[key] = value
    return start_station_name_dict

def line_dict(routes_file):
    line_num_dict = {}
    for index, row in routes_file.iterrows():
        key = row['name']
        line_num_dict[key] = index
    return line_num_dict

def all_station_list(all_stations_file):
    all_stations_file= gpd.GeoDataFrame(all_stations_file.copy())
    all_stations_name = all_stations_file[['name', 'geometry', 'geo_x', 'geo_y']].reset_index(drop=True)
    return all_stations_name
    
# Sorting the stations on each route
def order_route(first_stop, unordered_route):
    new_order = []
    remaining_route = unordered_route.drop(unordered_route[unordered_route.name == first_stop.name.values[0]].index).reset_index(drop=True)
    tree = shapely.STRtree(remaining_route.geometry)
    new_order.append(first_stop)
    for iter_ in range(len(remaining_route)):
        try:
            if iter_ == 0:
                nearest_station = pd.DataFrame(remaining_route.iloc[tree.nearest(first_stop.geometry)[0]]).T
                new_order.append(nearest_station)
                remaining_route = remaining_route.drop(remaining_route[remaining_route.name == nearest_station.name.values[0]].index).reset_index(drop=True)
                tree = shapely.STRtree(remaining_route.geometry)
            elif iter_ == 1:
                second_station = pd.DataFrame(remaining_route.iloc[tree.nearest(nearest_station.geometry)[0]]).T
                new_order.append(second_station)
                remaining_route = remaining_route.drop(remaining_route[remaining_route.name == second_station.name.values[0]].index).reset_index(drop=True)
                tree = shapely.STRtree(remaining_route.geometry)
            else:
                second_station = pd.DataFrame(remaining_route.iloc[tree.nearest(second_station.geometry)[0]]).T
                new_order.append(second_station)
                remaining_route = remaining_route.drop(remaining_route[remaining_route.name == second_station.name.values[0]].index).reset_index(drop=True)
                tree = shapely.STRtree(remaining_route.geometry)
        except TypeError:
            pass  # pass 'NoneType' object is not subscriptable Error
    return pd.concat(new_order).reset_index(drop=True)

def order_stations_inline(tram_line_dict,all_tram_stations_name,tram_routes,tram_start_station_name_dict):

    """
    Orders the tram stations based on the tram line routes and start station names.

    Args:
    tram_line_dict (dict): A dictionary containing tram line information.
    all_tram_stations_name (DataFrame): A DataFrame containing all tram station names.
    tram_routes (DataFrame): A DataFrame containing tram route information.
    tram_start_station_name_dict (dict): A dictionary mapping tram line names to their respective start station names.

    Returns:
    dict: A dictionary containing ordered tram station routes for each tram line.
    """
    
    tram_stations_dict = tram_line_dict.copy()

    for key,value in tram_stations_dict.items():
        value1 = value
        value2 = all_tram_stations_name.loc[all_tram_stations_name.within(tram_routes.iloc[value1].geometry.buffer(0.00000001))]
        value2['id'] = value2.reset_index().index
        tram_stations_dict[key] = value2[['id', 'name', 'geometry', 'geo_x', 'geo_y']].reset_index(drop=True)
    
    tram_stations_inorder_dict = tram_line_dict.copy()
    tram_order_route_dict = tram_line_dict.copy()

    for line in tram_stations_inorder_dict.keys():
        tram_stations_inorder = tram_stations_dict[line]
        for i in range(len(tram_stations_inorder)):
            if tram_stations_inorder.iloc[i]['name'] == tram_start_station_name_dict[line]:
                tram_stations_inorder_dict[line] = pd.DataFrame(tram_stations_inorder.iloc[i]).T
        tram_order_route_dict[line] = order_route(tram_stations_inorder_dict[line], tram_stations_dict[line])

    return tram_order_route_dict

def s_e_coordinates_pairs(s_e_coordinates):
    """
    Given a Pandas DataFrame `s_e_coordinates` with columns `s_coordinates_x`, `s_coordinates_y`, `e_coordinates_x`, 
    and `e_coordinates_y`, returns a new DataFrame with columns `s_coordinates` and `e_coordinates`.
    
    `s_coordinates` and `e_coordinates` are each a tuple of the form `(x, y)`, where `x` is the value in the 
    `s_coordinates_x` or `e_coordinates_x` column, and `y` is the value in the `s_coordinates_y` or `e_coordinates_y` 
    column.
    
    Parameters
    ----------
    s_e_coordinates : pandas.DataFrame
        A DataFrame with columns `s_coordinates_x`, `s_coordinates_y`, `e_coordinates_x`, and `e_coordinates_y`.
    
    Returns
    -------
    pandas.DataFrame
        A new DataFrame with columns `s_coordinates` and `e_coordinates`.
    """
    
    # Create an empty DataFrame to hold the new columns
    s_e_coordinates_pairs = pd.DataFrame()
    
    # Create the `s_coordinates` column by zipping the `s_coordinates_x` and `s_coordinates_y` columns
    s_e_coordinates_pairs['s_coordinates'] = list(zip(s_e_coordinates.s_coordinates_x, s_e_coordinates.s_coordinates_y))
    
    # Create the `e_coordinates` column by zipping the `e_coordinates_x` and `e_coordinates_y` columns
    s_e_coordinates_pairs['e_coordinates'] = list(zip(s_e_coordinates.e_coordinates_x, s_e_coordinates.e_coordinates_y))
    
    # Return the new DataFrame
    return s_e_coordinates_pairs


# Define nearest nodes with real_world coordinates of start and end points, obtain the nearest id pairs of nodes 
def find_nearest_node(coordinate, nodes):
    """
    Find the nearest node to a given coordinate or geometry in a GeoDataFrame of nodes.

    Args:
    - coordinate: a tuple of longitude and latitude (in decimal degrees) or a shapely geometry object representing the location to search from
    - nodes: a GeoDataFrame containing nodes with a 'geometry' column representing their coordinates as shapely Point objects
    
    Returns:
    - the id value of the nearest node to the input coordinate or geometry

    Example:
    >>> import geopandas
    >>> from shapely.geometry import Point
    >>> from shapely import Geometry
    >>> nodes = geopandas.read_file('nodes.shp')
    >>> coordinate = (-122.3, 47.6)
    >>> find_nearest_node(coordinate, nodes)
    1234

    Note: This function requires the shapely and STRtree libraries to be installed.
    """
    # Build an STRtree index of the nodes' geometries for efficient nearest-neighbor search
    node_tree = shapely.STRtree(nodes.geometry)
   
    # Find the nearest node to the input coordinate or geometry using the STRtree index
    if isinstance(coordinate, tuple):
        find_nearest = node_tree.nearest(shapely.points(coordinate))
    elif isinstance(coordinate, shapely.lib.Geometry):
        find_nearest = node_tree.nearest(coordinate)
    
    # Return the id value of the nearest node from the nodes GeoDataFrame
    # print(nodes.iloc[find_nearest[1]]['id'])
    return int(nodes.iloc[find_nearest]['id'])

def id_pairs(coordinates_pairs, nodes):
    """
    Map the start and end coordinates to the nearest nodes in a transportation network.

    Args:
    - coordinates_pairs: a pandas DataFrame containing start and end coordinates in the network, with columns 's_coordinates', 'e_coordinates'
    - nodes: a GeoDataFrame containing nodes in the network, with a 'geometry' column representing their coordinates as shapely Point objects

    Returns:
    - a pandas DataFrame containing the nearest node IDs for each start and end coordinate pair, with columns 's_id', 'e_id'

    """
    # Initialize an empty DataFrame to store the start and end node IDs
    id_pairs = pd.DataFrame(columns=['s_id','e_id'])
    pairs_num = coordinates_pairs.shape[0]
    
    # For each start and end coordinate pair, find the nearest node in the network and store the node IDs in the DataFrame
    for i in range(pairs_num):
        s_coordinate = shapely.points(coordinates_pairs.s_coordinates[i])
        s_id = find_nearest_node(s_coordinate, nodes)
        e_coordinate = shapely.points(coordinates_pairs.e_coordinates[i])
        e_id = find_nearest_node(e_coordinate, nodes)
        id_pairs.loc[i] = [s_id,e_id]
        
    return id_pairs

def id_pairs_inline(tram_line_dict,tram_order_route_dict):
    """
    Generates ID pairs for the starting and ending stations of tram lines.

    Args:
    tram_line_dict (dict): A dictionary containing tram line information.
    tram_order_route_dict (dict): A dictionary containing tram order route information.

    Returns:
    dict: A dictionary containing ID pairs for the starting and ending stations of tram lines.
    """
    tram_order_coordinates = tram_order_route_dict.copy()
    tram_order_coordinates = {key: value[['name', 'geo_x', 'geo_y']] for key, value in tram_order_coordinates.items()}


    new_dict = {}

    for key, value in tram_order_coordinates.items():
        shifted_value = pd.concat([value, value.shift(-1)], axis=1)
        shifted_value.columns = [f'col{i+1}' for i in range(len(value.columns))] + [f'col{i+4}' for i in range(len(value.columns))]
        new_dict[key] = shifted_value.dropna().reset_index(drop=True)
    
    tram_order_coordinates = {key: value[['col1', 'col4', 'col2', 'col3', 'col5', 'col6']] for key, value in new_dict.items()}

    for key, value in tram_order_coordinates.items():
        value.rename(columns={'col1': 's_name', 'col4': 'e_name','col2': 's_coordinates_x', 'col3': 's_coordinates_y', 'col5': 'e_coordinates_x', 'col6': 'e_coordinates_y'}, inplace=True)
    
    tram_order_coordinates_pairs = tram_order_coordinates.copy()
    tram_order_id_pairs = tram_order_coordinates.copy()

    for line in tram_order_coordinates.keys():
        tram_order_coordinates_pairs[line] = s_e_coordinates_pairs(tram_order_coordinates[line])
        tram_order_id_pairs[line] = id_pairs(tram_order_coordinates_pairs[line],nodes)
    
    return(tram_order_id_pairs)