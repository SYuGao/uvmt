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


# Plots even-indexed routes from a routes file along with edges and shortest path edges
def plot_routes_even(routes_file, edges, shortest_path_edges):
    """
    Plots routes and shortest paths on a map using matplotlib.

    Args:
        routes_file (pandas.DataFrame): DataFrame containing route information.
        edges (geopandas.GeoDataFrame): GeoDataFrame containing all edges.
        shortest_path_edges (dict): Dictionary containing shortest path edges.

    Returns:
        None
    """
    shortest_edges_list = list(shortest_path_edges.items())
    
    # num_plots = len(routes_file) // 2
    num_plots = len(shortest_path_edges) // 2
    
    colors = ['black', 'green', 'blue', 'orange', 'purple']
    
    fig, axes = plt.subplots(num_plots, 2, figsize=(12, 12))

    for i, ax in enumerate(axes.flat):
        
        if i < num_plots:
            # Select color based on the index to differentiate routes
            color = colors[i % len(colors)]
            
            # Plot all edges in gray with low opacity
            gpd.GeoDataFrame(edges.copy()).plot(ax=ax, color='gray', alpha=0.5)
            
            # Plot the shortest path edges with a color and line width based on count_weight
            gpd.GeoDataFrame(shortest_edges_list[i * 2][1].copy()).plot(ax=ax, zorder=1,
                                                                        linewidth=(shortest_edges_list[i * 2][1].count_weight) * 2,
                                                                        color=color)
            
            # Set the title of the subplot to the name of the route
            ax.set_title(routes_file.iloc[i * 2]['name'])
        else:
            # Turn off axes for extra subplots if the number of routes is odd
            ax.axis('off')

        # Turn off axes for all subplots
        ax.axis('off')

    plt.show()  # Display the plot


# Plots odd-indexed routes from a routes file along with edges and shortest path edges
def plot_routes_odd(routes_file, edges, shortest_path_edges):
    """
    Plots odd-indexed routes from a routes file along with edges and shortest path edges.

    Args:
        routes_file (pandas.DataFrame): A DataFrame containing route information.
        edges (geopandas.GeoDataFrame): A GeoDataFrame containing edge information.
        shortest_path_edges (dict): A dictionary of shortest path edges.

    Returns:
        None
    """

    shortest_edges_list = list(shortest_path_edges.items())

    # num_plots = len(routes_file) // 2
    num_plots = len(shortest_path_edges) // 2

    colors = ['black', 'green', 'blue', 'orange', 'purple']

    # Create subplots for the plots
    fig, axes = plt.subplots(num_plots, 2, figsize=(12, 12))

    for i, ax in enumerate(axes.flat):
        if i < num_plots:
            color = colors[i % len(colors)]  # Select color based on index

            # Plot edges and shortest path edges
            gpd.GeoDataFrame(edges.copy()).plot(ax=ax, color='gray', alpha=0.5)
            gpd.GeoDataFrame(shortest_edges_list[i * 2 + 1][1].copy()).plot(ax=ax, zorder=1,
                                                                             linewidth=(shortest_edges_list[i * 2 + 1][
                                                                                             1].count_weight) * 2,
                                                                             color=color)

            # Set the title for the plot based on the route name
            ax.set_title(routes_file.iloc[i * 2 + 1]['name'])
        else:
            ax.axis('off')  # Turn off the axis for blank plots

        ax.axis('off')  # Turn off the axis for all plots

    plt.show()  # Display the plot


# Plots each route from a routes file along with edges and shortest path edges
def plot_routes(routes_file, edges, shortest_path_edges):
    """
    Plots the routes on a map.

    Args:
        routes_file (DataFrame): DataFrame containing route information.
        edges (DataFrame): DataFrame containing edge information.
        shortest_path_edges (dict): Dictionary containing shortest path edges for each route.

    Returns:
        None
    """

    # Convert shortest_path_edges to a list of key-value pairs
    shortest_edges_list = list(shortest_path_edges.items())

    # Get the number of plots based on the number of routes in routes_file
    # num_plots = len(routes_file)
    num_plots = len(shortest_path_edges)

    # Define a list of colors for the routes
    colors = ['black', 'green', 'blue', 'orange', 'purple']

    # Create subplots with num_plots rows and 1 column, and set the figure size
    fig, axes = plt.subplots(num_plots, 1, figsize=(40, 40))

    # Iterate over the axes and plot the routes
    for i, ax in enumerate(axes.flat):
        if i < num_plots:
            # Select a color from the colors list based on the index
            color = colors[i % len(colors)]

            # Plot all edges on the map as gray lines with low transparency
            gpd.GeoDataFrame(edges.copy()).plot(ax=ax, color='gray', alpha=0.2)

            # Plot the shortest path edges for the current route with a specific color and width
            gpd.GeoDataFrame(shortest_edges_list[i][1].copy()).plot(ax=ax, zorder=1,
                                                                     linewidth=(shortest_edges_list[i][1].count_weight) * 2,
                                                                     color=color)

            # Set the title of the subplot as the route name
            ax.set_title(routes_file.iloc[i]['name'])
        else:
            # Turn off the axes for any extra subplots
            ax.axis('off')

        # Turn off the axes for all subplots
        ax.axis('off')

    # Show the plot
    plt.show()