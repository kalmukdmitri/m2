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
        for col in df:
            if type(df[col][0]) == numpy.bool_:
                df[col] =  df[col].apply(lambda x: 1 if x else 0)
                
            if type(df[col][0]) == datetime.date:
                df[col] = df[col].astype('<M8[ns]')
        
        dict_types = {
            dtype('O'): 'Nullable(String)',
            dtype('uint64'): 'Nullable(UInt64)',
            dtype('uint32'): 'Nullable(UInt32)',
            dtype('uint16'): 'Nullable(UInt16)',
            dtype('bool'): 'Int8',
            dtype('uint8'): 'Nullable(UInt8)',
            dtype('float64'): 'Nullable(Float64)',
            dtype('float32'): 'Nullable(Float32)',
            dtype('int64'): 'Nullable(Int64)',
            dtype('int32'): 'Nullable(Int32)',
            dtype('int16'): 'Nullable(Int16)',
            dtype('int8'):  'Nullable(Int8)',
            dtype('<M8[D]'): 'Date',
            datetime.date : 'Date',
            dtype('<M8[ns]'): 'DateTime'
        }

        query = f"CREATE TABLE IF NOT EXISTS {table_name}\n"
        

        cols  ='('
        df_columns = [[list(df.columns)[i],list(df.dtypes)[i]] for i in range(len(list(df.dtypes)))]
        print(df_columns)
        for df_col in df_columns:
            if df_col[0] in ("date","Date"):
                cols += df_col[0] + f' Date\n,'
            else:
                cols += df_col[0] + f' {dict_types[df_col[1]]}\n,'

        end_part = f''')
Engine = MergeTree 
PARTITION BY {partition} ORDER BY {partition}'''
        
        query = query+cols[:-2]+end_part
        print(query)
        creation_results = self.client.execute(query)
        return creation_results
    
    def insert(self, df, table_name, step=20000, logging = False):
        report_lenth = len(df)
        counter = 0
        operation_started = datetime.datetime.now()
        
        for col in df:
            if type(df[col][0]) == numpy.bool_:
                df[col] =  df[col].apply(lambda x: 1 if x else 0)
                
            if type(df[col][0]) == datetime.date:
                df[col] = df[col].astype('<M8[ns]')
        
        for i in range(0,report_lenth,step):
            print('Rows Left ' + str(report_lenth-counter))
            print('Operations left ' + str(int((report_lenth-counter)/step)))
            print('Time elapsed '+str(datetime.datetime.now() - operation_started))
            
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
            print(f'INSERT INTO {table_name} {tuple(df.columns)} VALUES'.replace("'",''))
            counter+=step
        
        return res
    
    def creat_table_df(self, df, table_name):
        res1 = self.create_table_query(df,table_name )
        res = self.insert(df,table_name)
        return res1, res
    
    def get_full_table(self, table_name):
        q = f'SELECT * FROM {table_name}'
        result, columns = self.client.execute(q, with_column_types = True)
        return pandas.DataFrame(result,columns=[i[0] for i in columns])
    
    def get_query_results(self, query):
        q = query
        result, columns = self.client.execute(q, with_column_types = True)
        return pandas.DataFrame(result,columns=[i[0] for i in columns])

    
class clickhouse_logger:
    
    def __init__(self, table_name):
        self.clk  = clickhouse_pandas('webapp')
        self.table_name = table_name 
        self.started = datetime.datetime.today()
        self.file_name = sys.argv[0] 
        self.comment = '' 
        self.errors = ''
        self.rows_recieved = 0 
        self.rows_updated = 0 
        self.data_start =  datetime.datetime.today().date()
        self.data_end =  datetime.datetime.today().date()
        
        
    def add_rows_recieved(self,rows_recieved):
        self.rows_recieved = rows_recieved
        
    def add_rows_updated(self,rows_updated):
        self.rows_updated = rows_updated
        
    def comment_add(self,comment_text):
        self.comment = comment_text
        
    def add_data_start(self,data_start):
        self.data_start = data_start
        
    def add_data_end(self,data_end):
        self.data_end = data_end
        
        
    def pandify(self):
        
        self.ended = datetime.datetime.today()
        columns = [ 
                    'date',
                    'table_name',
                    'file_name',
                    'started' ,
                    'ended',
                    'data_end',
                    'rows_recieved',
                    'rows_updated',
                    'errors',
                    'comment'
                  ]
        
        vals = [[self.data_start,
                 self.table_name, 
                 self.file_name,
                 self.started,
                 self.ended,  
                 self.data_end,
                 self.rows_recieved, 
                 self.rows_updated, 
                 self.errors, 
                 self.comment
                ]]
        self.log_pd = pandas.DataFrame(vals,columns= columns)
        return self.log_pd
    
    def errors_found(self, errors_text):
        
        self.errors = errors_text
        self.bq_save_log()
        
    def no_errors_found(self):
  
        self.errors = "no errors"
        self.bq_save_log()
        
    def bq_save_log(self):
        loggs = self.pandify()
        self.clk.insert(loggs, 'webapp.loggs')
