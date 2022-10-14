from clickhouse_py  import clickhouse_pandas, clickhouse_logger
from datetime import datetime
from ga_connector_click import ga_connect
from google.oauth2 import service_account
from googleapiclient.discovery import build
from clickhouse_driver import Client
from numpy import dtype
from oauth2client.service_account import ServiceAccountCredentials
import csv
import datetime
import json
import numpy
import os
import pandas
import requests
import string
import sys
import urllib
import pandas_gbq

# key_path = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\m2-main-cd9ed0b4e222.json'
key_path = '/home/kalmukds/m2-main-cd9ed0b4e222.json'
gbq_credential = service_account.Credentials.from_service_account_file(key_path,)
clk  = clickhouse_pandas('ga')

q = f"""SELECT * FROM schedules.view_refresh"""
tables = clk.get_query_results(q)
for scripts in tables.iterrows():
    script_str = scripts[1].script.split(';')
    for line in script_str:
        line = line.strip()
        if line != '':
            clk.get_query_results(line)
q= '''SELECT
  table_name
FROM
  EXPORT_CLICK.INFORMATION_SCHEMA.VIEWS;'''
export_tables = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)  
tables = ['`EXPORT_CLICK.'+f'{i}`' for i in export_tables['table_name']]

def upload_multipart(table_name, df):
    
    dates_list = list(set(df['date']))
    dates_list.sort()
    
    for i in range(0,len(dates_list), 10):
        
        date_block = dates_list[i:i+10]
        regs_date = df[df['date'].apply(lambda x: x in date_block)]
        regs_date = regs_date.reset_index(drop=True)
        clk.insert(regs_date, table_name)


for table in tables:
    
    q = f'''SELECT * FROM {table}'''
    table_df = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
    if 'date' not in table_df.columns:
        table_df['date'] = datetime.datetime.now().date()
    click_name = 'nikitindd.'+table.replace(' ','_').replace('EXPORT_CLICK.','BQ_').replace('`','')
    clear_q = f'ALTER TABLE {click_name} DELETE WHERE 1=1;'
    clk.get_query_results(clear_q)
    upload_multipart(click_name,table_df)