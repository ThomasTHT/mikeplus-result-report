#!/usr/bin/env python
# coding: utf-8

# In[1]:


# import of necessary libaries
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


# ----

# In[2]:


#location of mikeio
print(mikeio1d.__file__)


# ------

# # Function definition

# In[1]:


print('Function definition...')


# -----

# In[3]:


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


# Gets InvertLevel and GroundLevel from a Res1DManhole object:
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

def get_diameter(reach):
    try:
        grid_points = list(reach.GridPoints)
        h_point = grid_points[0]
        diameter = round(h_point.CrossSection.Diameter,2)
    except:
        diameter = None

    return diameter

def get_slope(reach):
    grid_points = list(reach.GridPoints)
    gp_first = grid_points[0]
    gp_last = grid_points[-1]
    length = reach.Length
    slope = ((gp_first.Z - gp_last.Z) / length)*100
    return abs(slope)

def get_data_item(reach, quantity_id):
    item = None

    for data_item in list(reach.DataItems):
        if data_item.Quantity.Id == quantity_id:
            item = data_item
            break

    return item


# Gets min and max value and times for a any model element (not only reaches); needs
#     DHI.Mike1D.ResultDataAccess.Res1DManhole object or similar
#     list of DateTime objects
#     string with quantity (default ist "Discharge")

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


def get_minmax_value(reach, simulation_start, quantity_id="Discharge"):    
    item_max = get_data_item(reach, quantity_id + "Max")
    item_max_time = get_data_item(reach, quantity_id + "MaxTime")
    item_min = get_data_item(reach, quantity_id + "Min")
    item_min_time = get_data_item(reach, quantity_id + "MinTime")

    items = [item_max, item_max_time, item_min, item_min_time]
    if None in items:
        return None

    min_value, min_time = None, None
    max_value, max_time = None, None   

    number_of_elements = item_min.TimeData.NumberOfElements
    time_step_index = 0

    for element_index in range(number_of_elements):
        value = item_min.TimeData.GetValue(time_step_index, element_index)
        time = item_min_time.TimeData.GetValue(time_step_index, element_index)
        
        if min_value is None or value < min_value:
            min_value = value 
            min_time = simulation_start.AddSeconds(time)

        value = item_max.TimeData.GetValue(time_step_index, element_index)
        time = item_max_time.TimeData.GetValue(time_step_index, element_index)

        if max_value is None or value > max_value:
            max_value = value 
            max_time = simulation_start.AddSeconds(time)

    return min_value, min_time.ToString(), max_value, max_time.ToString()

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

def q_max_q_full_ratio_data (q_minmax_data,qfull):
    try:
        qmax_qfull_ratio= q_minmax_data[2]/qfull

    except:
        q_minmax_data[2] is None or qfull is None
        qmax_qfull_ratio=None
    
    return qmax_qfull_ratio


# In[4]:


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
            
    

        


# -----------

# # Find res1d and sqlite files

# In[5]:


cwd = os.getcwd()

cwd


# In[6]:


myFiles = os.listdir(cwd)


# In[7]:


# create a list of res1d-files
myRes1dFiles = [file for file in myFiles if fnmatch(file, '*.res1d')]

# pick the first res1d-file
oneRes1dFile = myRes1dFiles[0]

oneRes1dFile


# In[8]:


# create a list of sqlite-files
mySQLiteFiles = [file for file in myFiles if fnmatch(file, '*.sqlite')] 


# pick the first sqlite-file
oneSQLiteFile = mySQLiteFiles[0]

oneSQLiteFile


# In[9]:


altres1d = Res1D(oneRes1dFile)


# In[10]:


altres1d


# In[11]:


#df = altres1d.read()


# # Create nodes and reaches lists from res1d

# In[12]:


# create a Res1d-object
res1d = Res1D(oneRes1dFile)

reaches = list(res1d.data.Reaches)
nodes = list(res1d.data.Nodes)

qfull_data = get_qfull_data(res1d)
times_list  = list(res1d.data.TimesList)

simulation_start = res1d.data.StartTime


# In[13]:


nodes


# In[14]:


myNode = nodes[-1]
myNode.ID


# In[15]:


# The list "nodes" contains special nodes at the end of weirs without defined end. 
# Their ID looks like 'Weir Outlet:B4.1510w1'. We want to eliminate those elements
# from the list.

#for i, n in enumerate(nodes):
#    try:
#        n.ID.split(':')[-2] == 'Weir Outlet'
#        nodes.pop(i)
#    except IndexError:
#        pass

#nodes


# # Prepare desired node results

# In[36]:


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
    


# In[17]:


# create dictionary with lists
dict_res1d_nodes = {'Node_ID':Node_ID, 'WLmin':WLmin, 'WLmax':WLmax, 'WLmaxmin':WLmaxmin}

# create dataframe, transpose
df_res1dNode=pd.DataFrame(dict_res1d_nodes)

df_res1dNode =  df_res1dNode.set_index('Node_ID')

df_res1dNode


# # Prepare desired Link results

# In[18]:


#initialize lists
Link_ID=[]
Reach_Type=[]
From_Node_ID=[]
Invert_level_from_Node=[]
Ground_level_from_Node=[]
To_Node_ID=[]
Invert_level_to_Node=[]
Ground_level_to_Node=[]
Diameter=[]
Length=[]
Slope=[]
Vmin=[]
t_Vmin=[]
Vmax=[]
t_Vmax=[]
Qmin=[]
t_Qmin=[]
Qmax=[]
t_Qmax=[]
Qfull=[]
Qmax_Qfull=[]
WLmax_start=[]
WLmax_start_time=[]
WLmax_end=[]
WLmax_end_time=[]



for reach in reaches:
    #call necessary informations from initially defined functions
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

    Qmax_Qfull_ratio = q_max_q_full_ratio_data(q_minmax_data,qfull)

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
    t_Vmin.append(v_minmax_data[1])
    Vmax.append(v_minmax_data[2])
    t_Vmax.append(v_minmax_data[3])
    Qmin.append(q_minmax_data[0])
    t_Qmin.append(q_minmax_data[1])
    Qmax.append(q_minmax_data[2])
    t_Qmax.append(q_minmax_data[3])
    Qfull.append(qfull)
    Qmax_Qfull.append(Qmax_Qfull_ratio)
    WLmax_start.append(maxWL[0])
    WLmax_start_time.append(maxWL[1])
    WLmax_end.append(maxWL[2])
    WLmax_end_time.append(maxWL[3])
    
#### Der Rest des Skripts war eingerückt, also Teil des for-loops. Aber das bedeutet
#### doch, dass der Dataframe nach jeder neuen Zeile erzeugt wird. Es reicht, wenn
#### man das am Ende macht.

#convert lists into list of lists and further into a dataframe    

# ASFA Liste deaktivieren
list_res1d_prop=[Link_ID,Reach_Type,From_Node_ID,Invert_level_from_Node,
                   Ground_level_from_Node,To_Node_ID,
                   Invert_level_to_Node,Ground_level_to_Node,
                   Diameter,Length,Slope,Vmin,t_Vmin,Vmax,t_Vmax,Qmin,t_Qmin,Qmax,t_Qmax,Qfull,Qmax_Qfull,WLmax_start,WLmax_start_time,WLmax_end,WLmax_end_time]


# ASFA: schreibe dictionary
# dict_res1d_prop = {'Link_ID':Link_ID, 'Reach_Type':Reach_Type, etc. etc.}


# ASFA: erstetze list_ durch dict_
df=pd.DataFrame(list_res1d_prop).transpose()

# define column names of final dataframe    
df.columns=['Haltungs-Nr.','Haltungstyp','Schacht oben','Sohlhöhe_Schacht oben',
                'Deckenhöhe_Schacht oben','Schacht unten','Sohlhöhe_Schacht unten',
                'Deckenhöhe_Schacht unten','Durchmesser [m]','Länge [m]','Gefälle [%]',
                'Vmin [m/s]','Vmin_Zeitpunkt','Vmax [m/s]','Vmax_Zeitpunkt','Qmin [m/s]',
                'Qmin_Zeitpunkt','Qmax [m/s]','Qmax_Zeitpunkt','Qvoll [m³/s]','Qmax/Qvoll [%]',
                'Wspmax Schacht oben','Wspmax Schacht oben Zeitpunkt','Wspmax Schacht unten','Wspmax Schacht Zeitpunkt']


# In[19]:


#print filename
print(res1d.file_path)

#visualize first 5 rows of dataframe
df.head()


# ---

# In[21]:


df.shape


# In[22]:


# Behalte nur die Zeilen, in denen Reach_Type = 'Link' ist
# TODO Überlgen, wie wir mit den Knotentypen und Haltungstypen umgehen; durch den Join am Ende wird
# scheinbar ohnehin alles bereinigt.
# ASFA ändern auf Reach_Type
df_res1dLink = df.loc[df['Haltungstyp'] == 'Link']

# ASFA: index auf Link_ID
# set index on Link_ID:
df_res1dLink =  df_res1dLink.set_index('Haltungs-Nr.')

df_res1dLink


# # Join Node results with msm_Node

# In[23]:


# establish connection to MIKE+ database:
con = sqlite3.connect(oneSQLiteFile)


# In[24]:


# pick two columns from 'msm_Link':
df_msmNode = pd.read_sql_query("SELECT muid, description, assetname from msm_Node", con)


# In[25]:


# set index on 'muid'
df_msmNode = df_msmNode.set_index('muid')


# In[26]:


df_msmNode.head()


# In[27]:


# join...werden dadurch die fälschlichen Knoten der Wehre entfernt?
df_res1dmsmNode = pd.merge(df_res1dNode, df_msmNode, left_index=True, right_index=True)

df_res1dmsmNode


# # Join Link results with msmLink

# In[28]:


# establish connection to MIKE+ database:
con = sqlite3.connect(oneSQLiteFile)


# In[29]:


# pick two columns from 'msm_Link':
df_msmLink = pd.read_sql_query("SELECT muid, description, assetname from msm_Link", con)


# In[30]:


# set index on 'muid'
df_msmLink = df_msmLink.set_index('muid')


# In[31]:


df_msmLink.head()


# In[32]:


df_res1dmsmLink = pd.merge(df_res1dLink, df_msmLink, left_index=True, right_index=True)

df_res1dmsmLink


# # define output res1d-report format (*.csv | *.xlsx)

# ---

# In[33]:


# ASFA: testen mit Manz...was ist beim Excel schiefgegangen?

# extract the root of the filename even if filename contains a dot, hence better than split('.')
rootName = os.path.splitext(oneRes1dFile)[0]

# export NODE result table to csv
df_res1dmsmNode.to_csv(rootName + '_Knoten.csv', index_label='MUID',header=['Min.WSP','Max.WSP','Max/Min.WSP','AssetName','Beschreibung'])

print(rootName + '_Knoten.csv exportiert')

# export LINK result table to csv
df_res1dmsmLink.to_csv(rootName + '_Haltungen.csv', index_label='MUID')

print(rootName + '_Haltungen.csv exportiert')

#export result table to csv without index, a specified sheeetname & 2 decimal places
#df.to_excel(res1d.file_path.split('.')[0]+'.xlsx',index=False, sheet_name='res1d_network_properties',float_format = "%0.3f")


# In[34]:


# os.system("pause")


# In[35]:


# Eingabe abwarten, um Kosole zu schließen
input('Zum Beenden ENTER druecken')

