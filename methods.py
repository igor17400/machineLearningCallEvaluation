import pandas as pd
import numpy as np
from matplotlib.ticker import FuncFormatter
import matplotlib.pyplot as plt
import matplotlib.lines as lines
import numpy as np
import json
import os
import shutil
from matplotlib import cm
import seaborn as sns
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
import random
from ip2geotools.databases.noncommercial import DbIpCity
import plotly.graph_objects as go

def copyFiles(src, dst):
    src_files = os.listdir(src)
    for file_name in src_files:
        full_file_name = os.path.join(src, file_name)
        if os.path.isfile(full_file_name):
            shutil.copy(full_file_name, dst)

def generateJsonFromLog(folder_path):
    ''' This method is responsible for converting .log files to .json files. Since
    python libraries work better with json files. '''
    files = []
    for r, d, f in os.walk(folder_path):
        for file in f:
            if '.log' in file:
                files.append(os.path.join(r, file))
    
    for f in files:
        pre, ext = os.path.splitext(f)
        os.rename(f, pre + '.json')


def deleteFilesEqualNames(src):
    '''This method is used to remove duplicates files in the format such as callstats_XXXX_XXXX \(1\).log
    because they are duplicates files.'''
    files = []
    file_name = ''
    for r, d, f in os.walk(src):
        for file in f:
            for k in file.split():
                if "(1).json" == k:
                    os.remove(src + '/' + file_name + ' ' + k)
                file_name = k


def returnListJSON(folder_path):
    '''This method returns an array of all .json files inside of the logs folder.'''
    files = []
    for r, d, f in os.walk(folder_path):
        for file in f:
            if '.json' in file:
                files.append(os.path.join(r, file))
    return files


def getLogDF(list_logs):
    '''Returns a dataframe containing all the informations except from call's parameters.
    Informations such as alias, IP, status, rating, etc. Except for the data parameter, since
    it needs to be separated into a new dataframe in order to obtain a more organized and easier 
    to work structure'''
    df = pd.DataFrame()
    frames = []
    out_str = ''
    
    for i in range(len(list_logs)):
        with open(list_logs[i]) as f:
            try:
                file_json = json.load(f)
                df = pd.json_normalize(file_json)
                df = df.drop(['data'], axis=1)
                df = df.drop_duplicates()
                frames.append(df)
            except:
                out_str += str(i) + '\t'
                #print(list_logs[i])
    
    # print(out_str)
    df = pd.concat(frames)
    df = df.reset_index()
    
    df['rating'] = df['rating'].astype('str')
    df['issue'] = df['issue'].astype('str')
    df['comment'] = df['comment'].astype('str')
    
    return df


def getLogDFNewVersion(list_logs):
    '''This method does the same thing as its previous version - GetLogDF.
    However, it will create a different dataframe for the new version of
    logs format'''
    df = pd.DataFrame()
    frames = []
    out_str = ''
    
    for i in range(len(list_logs)):
        with open(list_logs[i]) as f:
            try:
                file_json = json.load(f)
                df = pd.json_normalize(file_json)
                if 'log_session_id' in df.columns:
                    df = df.drop(['data'], axis=1)
                    df = df.drop_duplicates()
                    frames.append(df)
            except:
                out_str += str(i) + '\t'
                #print(list_logs[i])
    
    # print(out_str)
    df = pd.concat(frames)
    df = df.reset_index()
    
    return df


def readCallRating(list_logs):
    '''The goal of this function is to read all the call feedback files in the logs folder
    and then join the informations from those files such as rating, issue and comment into 
    the df_log dataframe using the function joinCallFeedBackDfLog.'''
    
    df = pd.DataFrame()
    frames = []
    out_str = ''
    
    for i in range(len(list_logs)):
        with open(list_logs[i]) as f:
            if list_logs[i][-11:] == 'rating.json':
                try:
                    file_json = json.load(f)
                    df = pd.json_normalize(file_json)
                    df = df.drop_duplicates()
                    frames.append(df)
                except:
                    out_str += str(i) + '\t'

                df = pd.concat(frames)
                df = df.reset_index()
    # print(out_str)
    
    return df

def joinCallFeedBackDfLog(df_call_rating, df_log):
    '''This function will simply join two dataframes. The callfeedback dataframe and
    the df_log dataframe based on each ID.'''
    
    df = pd.DataFrame()
    frames = []
    df_c = df_log.copy()

    df_call_rating['comment'] = df_call_rating['comment'].astype('str')
    
    for i in range(df_call_rating.shape[0]):
        df = df_c.loc[df_c.log_session_id == df_call_rating['log_session_id'].iloc[i]]
        df = df.drop(['index'], axis=1)
        # frames.append(df)
        
        if df.shape[0] > 0:
            df_c.at[df.index[0], 'rating'] = df_call_rating['rating'].iloc[i]
            df_c.at[df.index[0], 'issue'] = df_call_rating['issue'].iloc[i]
            df_c.at[df.index[0], 'comment'] = df_call_rating['comment'].iloc[i]
    
    # df = pd.concat(frames)
    
    return df_c

def genCallParametersDF(list_logs):
    '''This method generates a single dataframe with all the call's informations
    obtained from every .json file. The general idea here was to unify all variables 
    in a single dataframe and appeding to it an alias and a timestamp. This way, 
    it is possible to unique identify every column in the dataframe.'''
    
    out_str = ''
    
    data = {'time': [], 
            'latency': [], 
            'jitter_rx': [],
            'jitter_tx': [],
            'packet_loss_rx': [],
            'packet_loss_tx': [],
            'alias': [],
            'call_id':[],
            'log_session_id':[],} 
    df = pd.DataFrame(data)
    frames = []
    for i in range(len(list_logs)):
        with open(list_logs[i]) as f:
            try:
                file_json = json.load(f)
                
                df_json = pd.json_normalize(file_json['data'])
                df_json['alias'] = file_json['alias']
                df_json['timestamp'] = file_json['timestamp']
                df_json['status'] = file_json['status']
                df_json['address'] = file_json['address']
                df_json['version'] = file_json['version']
                df_json['call_id'] = file_json['call_id']
                
                if 'log_session_id' in file_json:
                    df_json['log_session_id'] = file_json['log_session_id']
                else:
                    df_json['log_session_id'] = 'empty'
                    
                frames.append(df_json)
            except:
                out_str += str(i) + '\t'
                # out_str += str(list_logs[i]) + '\n'
    
    # print(out_str)
    df = pd.concat(frames)
    df = df.reset_index()
    
    return df


def genCallParametersDFNewVersion(list_logs):
    '''This method does the same thing as its previous version - genCallParametersDF.
    However, it will only contain occurences for the new version.'''
    
    out_str = ''
    
    data = {'time': [], 
            'latency': [], 
            'jitter_rx': [],
            'jitter_tx': [],
            'packet_loss_rx': [],
            'packet_loss_tx': [],
            'alias': [],
            'call_id':[],
            'log_session_id':[],} 
    df = pd.DataFrame(data)
    frames = []
    for i in range(len(list_logs)):
        with open(list_logs[i]) as f:
            try:
                file_json = json.load(f)
                df_if = pd.json_normalize(file_json)
                if 'log_session_id' in df_if.columns:
                    df_json = pd.json_normalize(file_json['data'])
                    df_json['alias'] = file_json['alias']
                    df_json['timestamp'] = file_json['timestamp']
                    df_json['status'] = file_json['status']
                    df_json['address'] = file_json['address']
                    df_json['version'] = file_json['version']
                    df_json['call_id'] = file_json['call_id']
                    df_json['log_session_id'] = file_json['log_session_id']

                    frames.append(df_json)
            except:
                out_str += str(i) + '\t'
                # out_str += str(list_logs[i]) + '\n'
    
    # print(out_str)
    df = pd.concat(frames)
    df = df.reset_index()
    
    return df


def getOneFile(df, alias, timestamp):
    '''Function to ge one occurence in the dataframe based on the two paramaters that unique
    identify a call. In other words, by filtering the call based on alias and timestamp we can 
    get a unique call'''
    df_ = df[df['alias'] == alias]
    df_ = df_[df_['timestamp'] == timestamp]
    
    return df_

def getRating(alias, timestamp):
    '''Method to get the rating of a specific call based on the two parameters that unique 
    identify a call.'''
    df_ = df_log[df_log['alias'] == alias]
    df_ = df_[df_['timestamp'] == timestamp]
    
    return df_['rating'].iloc[0]

def getAliasArr(df):
    alias_arr = []
    for alias in df['alias'].tolist():
        if alias not in alias_arr:
            alias_arr.append(alias)
    return alias_arr

def createFolder(path):
    try:
        os.mkdir(path)
    except OSError:
        print ("Creation of the directory %s failed" % path)

def createAliasFolders(df_log):
    import os.path 
    df = df_log.copy()
    
    # detect the current working directory and print it
    path = os.getcwd()
    path += '/images/ChartsPerAlias/'
    folders_names = ['JitterRx', 'JitterTx', 'Latency', 'PacketLossRx', 'PacketLossTx']
    
    ## Array with all unique alias from the dataframe
    alias_arr = getAliasArr(df)
    for alias in alias_arr:
        path = os.getcwd()
        path += '/images/ChartsPerAlias/'
        path += str(alias)
        if not os.path.isdir(path):
            try:
                createFolder(path)
                for folder in folders_names:
                    createFolder(path + '/' + str(folder))
            except:
                continue