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

def plot_routes_even(routes_file, edges, shortest_path_edges):
    shortest_edges_list = list(shortest_path_edges.items())
    num_plots = len(routes_file) // 2
    colors = ['black', 'green', 'blue', 'orange', 'purple']
    fig, axes = plt.subplots(num_plots, 2, figsize=(25, 25))

    for i, ax in enumerate(axes.flat):
        if i < num_plots:
            
            color = colors[i % len(colors)]

            gpd.GeoDataFrame(edges.copy()).plot(ax=ax, color='gray', alpha=0.5)
            gpd.GeoDataFrame(shortest_edges_list[i * 2][1].copy()).plot(ax=ax, zorder=1,
                                                                               linewidth=(shortest_edges_list[i * 2][
                                                                                               1].count_weight) * 2,
                                                                               color=color)
            ax.set_title(routes_file.iloc[i * 2]['name'])
        else:
            ax.axis('off')

        ax.axis('off')

    plt.show()

def plot_routes_odd(routes_file, edges, shortest_path_edges):
    shortest_edges_list = list(shortest_path_edges.items())
    num_plots = len(routes_file) // 2
    colors = ['black', 'green', 'blue', 'orange', 'purple']
    fig, axes = plt.subplots(num_plots, 2, figsize=(25, 25))

    for i, ax in enumerate(axes.flat):
        if i < num_plots:
            
            color = colors[i % len(colors)]

            gpd.GeoDataFrame(edges.copy()).plot(ax=ax, color='gray', alpha=0.5)
            gpd.GeoDataFrame(shortest_edges_list[i * 2 + 1][1].copy()).plot(ax=ax, zorder=1,
                                                                               linewidth=(shortest_edges_list[i * 2 + 1][
                                                                                               1].count_weight) * 2,
                                                                               color=color)
            ax.set_title(routes_file.iloc[i * 2 + 1 ]['name'])
        else:
            ax.axis('off')

        ax.axis('off')

    plt.show()