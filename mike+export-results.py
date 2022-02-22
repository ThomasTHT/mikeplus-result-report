#!/usr/bin/env python
# coding: utf-8

# Script based on an initial version by Gediminas Kiršanskas gedaskir


# # Import necessary libraries

print('Importing necessary libraries...')


# MIKE IO 1D, needs Pandas and Numpy
import mikeio1d
from mikeio1d.res1d import Res1D, QueryDataNode, QueryDataReach,ResultData, mike1d_quantities, ResultData
import pandas as pd
import numpy as np

# connection to MIKE+ database:
import sqlite3

# file and folder manipulation for input and output:
import os

# find files faster
from fnmatch import fnmatch


# print location of mikeio:
# print(mikeio1d.__file__)


# # Define functions

print('Defining functions...')


# get Discharge of full reach:
def get_qfull_data(res1d):
    network_datas = list(res1d.data.NetworkDatas)
    for data in network_datas:
        if data.Quantity.Id == "Discharge of full reach":
            return data    

def get_qfull(reach, qfull_data):
    if qfull_data is None:
        return None

    reach_data = qfull_data.GetReachData(reach.Name)

    if reach_data is not None:
        return reach_data.GlobalValue

    return None


# return InvertLevel and GroundLevel from a Res1DManhole object:
def get_node_levels(node):
    try:
        invert_level = node.BottomLevel
    except:
        invert_level = None

    try:
        ground_level = node.GroundLevel
    except:
        ground_level = None

    return invert_level, ground_level


# return Diameter of reach looking at the first GridPoint, with two decimal places
# needs modification for rectangular and CRS reaches:
def get_diameter(reach):
    try:
        grid_points = list(reach.GridPoints)
        h_point = grid_points[0]
        diameter = round(h_point.CrossSection.Diameter,2)
    except:
        diameter = None

    return diameter


# compute slope from first and last GridPoint, returns absolute value in percent:
def get_slope(reach):
    grid_points = list(reach.GridPoints)
    gp_first = grid_points[0]
    gp_last = grid_points[-1]
    length = reach.Length
    slope = ((gp_first.Z - gp_last.Z) / length)*100
    return abs(slope)


# returns the time series (?) for a specific Quantity ID
def get_data_item(reach, quantity_id):
    item = None

    for data_item in list(reach.DataItems):
        if data_item.Quantity.Id == quantity_id:
            item = data_item
            break

    return item


# Get min and max value and times for a any model element (not only reaches)
#       reach...DHI.Mike1D.ResultDataAccess.Res1DManhole object or similar
#       timeslist...list of DateTime objects
#       quantity_id...string with quantity (default ist "Discharge")
def get_minmax_value_result_file(reach, times_list, quantity_id="Discharge"):
    item = get_data_item(reach, quantity_id)

    min_value, min_time = None, None
    max_value, max_time = None, None   
    
    try:
        time_data = item.TimeData
        for time_step_index in range(time_data.NumberOfTimeSteps):
            
            for element_index in range(time_data.NumberOfElements):
                value = time_data.GetValue(time_step_index, element_index)
                if min_value is None or value < min_value:
                    min_value = value 
                    min_time = times_list[time_step_index].ToString()

                if max_value is None or value > max_value:
                    max_value = value 
                    max_time =  times_list[time_step_index].ToString()
    
    except:
        time_data = None
    
    return min_value, min_time, max_value, max_time


# not used; probably an early version of get_minmax_value_result_file

# def get_minmax_value(reach, simulation_start, quantity_id="Discharge"):    
#     item_max = get_data_item(reach, quantity_id + "Max")
#     item_max_time = get_data_item(reach, quantity_id + "MaxTime")
#     item_min = get_data_item(reach, quantity_id + "Min")
#     item_min_time = get_data_item(reach, quantity_id + "MinTime")

#     items = [item_max, item_max_time, item_min, item_min_time]
#     if None in items:
#         return None

#     min_value, min_time = None, None
#     max_value, max_time = None, None   

#     number_of_elements = item_min.TimeData.NumberOfElements
#     time_step_index = 0

#     for element_index in range(number_of_elements):
#         value = item_min.TimeData.GetValue(time_step_index, element_index)
#         time = item_min_time.TimeData.GetValue(time_step_index, element_index)
        
#         if min_value is None or value < min_value:
#             min_value = value 
#             min_time = simulation_start.AddSeconds(time)

#         value = item_max.TimeData.GetValue(time_step_index, element_index)
#         time = item_max_time.TimeData.GetValue(time_step_index, element_index)

#         if max_value is None or value > max_value:
#             max_value = value 
#             max_time = simulation_start.AddSeconds(time)

#     return min_value, min_time.ToString(), max_value, max_time.ToString()


# return max value and time for start and end of reach
# works probably not only for WaterLevel
# get_reach_start_values must be some predefined method for res1d
def max_WL_start_end (quantity):
    try:
        #np-array of waterlevels
        wl_start = res1d.get_reach_start_values(reach.Name, quantity)
        wl_end = res1d.get_reach_end_values(reach.Name, quantity)

        #max values
        wl_start_max = wl_start.max()
        wl_end_max = wl_end.max()

        #corresponding time
        time_start=str(res1d.time_index[np.argmax(wl_start)])
        time_end=str(res1d.time_index[np.argmax(wl_start)])
        
    except:
        wl_start,wl_end,wl_start_max,wl_end_max,time_start,time_end= None, None, None, None, None, None
    
    return wl_start_max, time_start, wl_end_max, time_end


# compute a/b but return None if a or b is None:
def get_ratio(a, b):
    try:
        ratio = a/b
        
    except:
        a is None or b is None
        ratio=None
    
    return ratio


# returns type of reach
# needs additional types like valve or orifice
def get_reach_type(reach):
    try:
        # get the full identifier string:
        fullstring = str(reach)
        
        # might be:
        # 'Res1DReach: B4.1520l1-13 (0-235.000625581015)'
        # 'Res1DReach: Weir:B4.1480w1-14 (0-1)',
        # 'Res1DReach: Pump:B4.1510p1-16 (0-80.0006092712283)'

        # get the second part after the ':'
        structureReach = fullstring.split(':')[1].lstrip()

        # if the second part is Pump or Weir, use Pump or Weir:
        if structureReach in ['Pump','Weir']:
            reach_type = structureReach

        # in any other case this must be a normal Link (Pipe or Canal or River)
        else:
            reach_type = 'Link'
    
    except:
        reach_type = None

    return reach_type


# # Find res1d and sqlite files

print('Searching res1d and sqlite files...')


cwd = os.getcwd()

# cwd


myFiles = os.listdir(cwd)


# create a list of res1d-files
myRes1dFiles = [file for file in myFiles if fnmatch(file, '*.res1d')]

# pick the first res1d-file
oneRes1dFile = myRes1dFiles[0]

print('Current res1d-file: ' + oneRes1dFile)


# create a list of sqlite-files
mySQLiteFiles = [file for file in myFiles if fnmatch(file, '*.sqlite')] 

# pick the first sqlite-file
oneSQLiteFile = mySQLiteFiles[0]

print('Current MIKE+ database: ' + oneSQLiteFile)


# # Create nodes and reaches lists from res1d

print('Creating nodes and links lists from res1d...')


# create a Res1d-object
res1d = Res1D(oneRes1dFile)

reaches = list(res1d.data.Reaches)
nodes = list(res1d.data.Nodes)

qfull_data = get_qfull_data(res1d)
times_list  = list(res1d.data.TimesList)

simulation_start = res1d.data.StartTime


# # Prepare desired node results

print('Preparing desired node results...')


# initialize node lists
Node_ID = []
WLmin = []
WLmax = []
WLmaxmin = []


# call necessary informations from initially defined functions
for node in nodes:
    id = node.ID
    min_wl = get_minmax_value_result_file(node, times_list, quantity_id="WaterLevel")[0]
    max_wl = get_minmax_value_result_file(node, times_list, quantity_id="WaterLevel")[2]
    max_min_wl = max_wl / min_wl
    
    # fill initialized node lists    
    Node_ID.append(id)
    WLmin.append(min_wl)
    WLmax.append(max_wl)
    WLmaxmin.append(max_min_wl)
    


# create dictionary with node lists
dict_res1d_nodes = {'Node_ID':Node_ID, 'WLmin':WLmin, 'WLmax':WLmax, 'WLmaxmin':WLmaxmin}


# create dataframe from dictionaty
df_res1dNode = pd.DataFrame(dict_res1d_nodes)
df_res1dNode = df_res1dNode.set_index('Node_ID')

# # Prepare desired Link results

print('Preparing desired link results...')


#initialize lists
Link_ID=[]
Reach_Type=[]
From_Node_ID=[]
Invert_level_from_Node=[]
Ground_level_from_Node=[]
To_Node_ID=[]
Invert_level_to_Node=[]
Ground_level_to_Node=[]
Length=[]
Diameter=[]
Slope=[]
Qfull=[]

Vmin=[]
Vmin_time=[]
Vmax=[]
Vmax_time=[]
Qmin=[]
Qmin_time=[]
Qmax=[]
Qmax_time=[]

Qmax_Qfull=[]
WLmax_start=[]
WLmax_start_time=[]
WLmax_end=[]
WLmax_end_time=[]


# call necessary informations from initially defined functions
for reach in reaches:

    name = reach.Name
    reachtype = get_reach_type(reach)
    node_from_index = reach.StartNodeIndex
    node_to_index = reach.EndNodeIndex

    node_from = nodes[node_from_index]
    node_to = nodes[node_to_index]

    invert_level_from, ground_level_from = get_node_levels(node_from)
    invert_level_to, ground_level_to = get_node_levels(node_to)

    diameter = get_diameter(reach)

    slope = get_slope(reach)

    qfull = get_qfull(reach, qfull_data)

    v_minmax_data = get_minmax_value_result_file(reach, times_list, "FlowVelocity")
    q_minmax_data = get_minmax_value_result_file(reach, times_list, "Discharge")
       
    maxWL=max_WL_start_end('WaterLevel')

    Qmax_Qfull_ratio = get_ratio(q_minmax_data[2],qfull)

    #fill initialized lists
    Link_ID.append(name)
    Reach_Type.append(reachtype)
    From_Node_ID.append(node_from.ID)
    Invert_level_from_Node.append(invert_level_from)
    Ground_level_from_Node.append(ground_level_from)
    To_Node_ID.append(node_to.ID)
    Invert_level_to_Node.append(invert_level_to)
    Ground_level_to_Node.append(ground_level_to)
    Diameter.append(diameter)
    Length.append(reach.Length)
    Slope.append(slope)
    Vmin.append(v_minmax_data[0])
    Vmin_time.append(v_minmax_data[1])
    Vmax.append(v_minmax_data[2])
    Vmax_time.append(v_minmax_data[3])
    Qmin.append(q_minmax_data[0])
    Qmin_time.append(q_minmax_data[1])
    Qmax.append(q_minmax_data[2])
    Qmax_time.append(q_minmax_data[3])
    Qfull.append(qfull)
    Qmax_Qfull.append(Qmax_Qfull_ratio)
    WLmax_start.append(maxWL[0])
    WLmax_start_time.append(maxWL[1])
    WLmax_end.append(maxWL[2])
    WLmax_end_time.append(maxWL[3])
    


# create dictionary with link lists
dict_res1d_links = {'Link_ID':Link_ID, 'Reach_Type':Reach_Type, 'From_Node_ID':From_Node_ID,'Invert_level_from_Node':Invert_level_from_Node, 
                   'Ground_level_from_Node':Ground_level_from_Node,'To_Node_ID':To_Node_ID, 'Invert_level_to_Node': Invert_level_to_Node,
                   'Ground_level_to_Node':Ground_level_to_Node, 'Diameter': Diameter,'Length':Length, 'Slope':Slope,'Vmin':Vmin, 
                   'Vmin_time':Vmin_time,'Vmax':Vmax, 'Vmax_time':Vmax_time, 'Qmin':Qmin, 'Qmin_time':Qmin_time,'Qmax':Qmax, 'Qmax_time':Qmax_time,
                   'Qfull':Qfull,'Qmax_Qfull':Qmax_Qfull, 'WLmax_start':WLmax_start, 'WLmax_start_time':WLmax_start_time, 'WLmax_end':WLmax_end,
                   'WLmax_end_time':WLmax_end_time}


# create dataframe from dictionary
df_res1dLink=pd.DataFrame(dict_res1d_links)

# keep only those lines where ReacH_Type = 'Link'
# df_res1dLink = df_res1dLink.loc[df['Reach_Type'] == 'Link']
# currently obsolete, as below merge eliminates non-Links

# set index on Link_ID:
df_res1dLink =  df_res1dLink.set_index('Link_ID')

# # Join Node results with msm_Node

print('Joining results with msm_Node information...')


# establish connection to MIKE+ database:
con = sqlite3.connect(oneSQLiteFile)


# pick two columns from 'msm_Link':
df_msmNode = pd.read_sql_query("SELECT muid, description, assetname from msm_Node", con)


# set index on 'muid'
df_msmNode = df_msmNode.set_index('muid')


# join...werden dadurch die fälschlichen Knoten der Wehre entfernt?
df_res1dmsmNode = pd.merge(df_res1dNode, df_msmNode, left_index=True, right_index=True)

# # Join Link results with msmLink

print('Joining Link results with msm_Link information...')


# establish connection to MIKE+ database:
con = sqlite3.connect(oneSQLiteFile)


# pick two columns from 'msm_Link':
df_msmLink = pd.read_sql_query("SELECT muid, description, assetname from msm_Link", con)


# set index on 'muid'
df_msmLink = df_msmLink.set_index('muid')


df_res1dmsmLink = pd.merge(df_res1dLink, df_msmLink, left_index=True, right_index=True)

# # define output res1d-report format (*.csv | *.xlsx)

print('Preparing your output...')


# extract the root of the filename even if filename contains a dot, hence better than split('.')
rootName = os.path.splitext(oneRes1dFile)[0]


# export node results

# available columns (alphabetical order)
# --------------------------------------
# assetname
# description
# WLmin
# WLmax
# WLmaxmin

# MUID is index and always exported as the first column

# You may modify myNodeColums using above names.

myNodeColumns = [
    'assetname',
    'WLmin',
    'WLmax']

# If you modify myNodeColumns, you should modify myNodeHeaderXX too.

myNodeHeaderDE = [
    'AssetName',
    'WSPmin',
    'WSPmax']

myNodeHeaderEN = [
    'AssetName',
    'WLmin',
    'WLmax']

# export NODE result table to csv and round to 3 decimal places
# change language of header as desired:
df_res1dmsmNode.to_csv(rootName + '_Nodes.csv', index_label='MUID', columns = myNodeColumns, header = myNodeHeaderDE, float_format="%.3f")

print(rootName + '_Nodes.csv exported')


# export LINK result table to csv

# available columns (aphabetical order)
# -------------------------------------
# assetname
# description
# Diameter
# From_Node_ID
# Ground_level_from_Node
# Ground_level_to_Node
# Invert_level_from_Node
# Invert_level_to_Node
# Length
# Qfull
# Qmax
# Qmax_Qfull
# Qmax_time
# Qmin
# Qmin_time
# Reach_Type
# Slope
# To_Node_ID
# Vmax
# Vmax_time
# Vmin
# Vmin_time
# WLmax_end
# WLmax_end_time
# WLmax_start
# WLmax_start_time

# Link_ID is index and always exported as the first column

# You may modify myLinkColums using above names.
myLinkColumns = [
    'assetname',
    'From_Node_ID',
    'To_Node_ID',
    'Length',
    'Diameter',
    'Slope',
    'Qfull',
    'WLmax_start',
    'WLmax_end',
    'Qmax',
    'Qmax_Qfull',
    'Qmax_time']
    
# If you modify myLinkColumns, you should modify myLinkHeaderXX too.    
myLinkHeaderDE = [
    'AssetName',
    'Von Knoten',
    'Nach Knoten',
    'Länge [m]',
    'Profilhöhe [m]',
    'Sohlgefälle [%]',
    'Qvoll [m3/s]',
    'WSPmax obem [m]',
    'WSPmax unten [m]',
    'Qmax [m3/s]',
    'Qmax/Qvoll [ ]',
    'Qmax Zeit']

myLinkHeaderEN = [
    'AssetName',
    'From Node',
    'To Node ',
    'Length [m]',
    'Height [m]',
    'Bed slope [%]',
    'Qfull [m3/s]',
    'WLmax start [m]',
    'WLmax end [m]',
    'Qmax [m3/s]',
    'Qmax/Qfull [ ]',
    'Qmax time']

# export LINK result table to csv and round to 3 decimal places
# change language of header as desired:
df_res1dmsmLink.to_csv(rootName + '_Links.csv', index_label='MUID', columns=myLinkColumns, header=myLinkHeaderDE, float_format="%.3f")

print(rootName + '_Links.csv exported')


# Wait for input. If the script was started with double click, the command window will close.
# If the script was started within the command window, the window remains open.
input('Press ENTER to finish')


