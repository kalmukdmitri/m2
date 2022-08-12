import requests
import numpy
from numpy import dtype
import pandas as pd
from datetime import datetime
import os
from clickhouse_driver import Client
import json
import csv
import requests
import datetime
import pandas
import string
from collections import OrderedDict

class clickhouse_pandas:
    #   Задаём ключ из файла
    key_path = '/home/kalmukds/m2-main-cd9ed0b4e222.json'
    key_path_extra = '/home/kalmukds/other_keys.json'
#     key_path = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\m2-main-cd9ed0b4e222.json'
#     key_path_extra = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\other_keys.json'
    f = open(key_path_extra, "r")
    key_other = f.read()
    keys = json.loads(key_other)['keys']['clickhouse']

    host = keys['db_host']
    user = keys['db_user']
    password = keys['db_password']
    database = keys['db_name']
    settings = {'input_format_null_as_default': True}
    
    
    def __init__(self, database='webapp'):

        self.client = client = Client(
            clickhouse_pandas.host,
            user = clickhouse_pandas.user,
            password= clickhouse_pandas.password,
            verify=False,
            database=database,
            settings=clickhouse_pandas.settings
        )
    
    def query(self, query):
        
        result, columns = self.client.execute(query, with_column_types = True)
        res = pd.DataFrame(result,columns=[i[0] for i in columns])
        return res
    
    
    def create_table_query(self, df, table_name, partition = 'date'):
        dict_types = {
            dtype('O'): 'Nullable(String)',
            dtype('uint64'): 'Nullable(UInt64)',
            dtype('uint32'): 'Nullable(UInt32)',
            dtype('uint16'): 'Nullable(UInt16)',
            dtype('bool'): 'Nullable(String)',
            dtype('uint8'): 'Nullable(UInt8)',
            dtype('float64'): 'Nullable(Float64)',
            dtype('float32'): 'Nullable(Float32)',
            dtype('int64'): 'Nullable(Int64)',
            dtype('int32'): 'Nullable(Int32)',
            dtype('int16'): 'Nullable(Int16)',
            dtype('int8'): 'Nullable(Int8)',
            dtype('<M8[D]'): 'Date',
            dtype('<M8[ns]'): 'DateTime'
        }

        query = f"CREATE TABLE IF NOT EXISTS {table_name}\n"

        cols  ='('
        df_columns = [[list(df.columns)[i],list(df.dtypes)[i]] for i in range(len(list(df.dtypes)))]
        for df_col in df_columns:
            if df_col[0] in ("date","Date"):
                cols += df_col[0] + f' Date\n,'
            else:
                cols += df_col[0] + f' {dict_types[df_col[1]]}\n,'

        end_part = f''')
Engine = MergeTree 
PARTITION BY {partition} ORDER BY {partition}'''
        
        creation_results = self.client.execute(query+cols[:-2]+end_part)
        return creation_results
    
    def insert(self, df, table_name, step=500):
        report_lenth = len(df)
        counter = 0
        
        for i in df:
            if type(df[i][0]) == numpy.bool_:
                df[i] =  df[i].astype(str)
        
        for i in range(0,report_lenth,step):

            if report_lenth <= counter+step:
                start = counter
                end = report_lenth
            else:
                start = counter
                end = counter+step-1
                
            slice_df = df.loc[start:end, :]
            
            inset_list = []
            for i in slice_df.itertuples():
                inset_list.append(list(i[1:]))
            
            res = self.client.execute( f'INSERT INTO {table_name} {tuple(df.columns)} VALUES'.replace("'",''),
            inset_list)
            
            counter+=step
        
        return res
    
    def creat_table_df(self, df, table_name):
        res1 = self.create_table_query(df,table_name )
        res = self.insert(df,table_name)
        return res1, res
    
    def get_full_table(self, table_name):
        q = f'SELECT * FROM webapp.{table_name}'
        result, columns = self.client.execute(q, with_column_types = True)
        return pandas.DataFrame(result,columns=[i[0] for i in columns])
    def get_query_results(self, query):
        q = query
        result, columns = self.client.execute(q, with_column_types = True)
        return pandas.DataFrame(result,columns=[i[0] for i in columns])