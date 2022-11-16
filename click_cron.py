from clickhouse_py  import clickhouse_pandas, clickhouse_logger
from datetime import datetime
from ga_connector_click import ga_connect
from sqlalchemy import create_engine
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

key_path_pg = '/home/kalmukds/pg_keys_special.json'
# key_path_pg = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\pg_keys_special.json'
f = open(key_path_pg, "r")
key_other = f.read()
keys = json.loads(key_other)['pg']

q = f"""SELECT * FROM schedules.view_refresh"""
tables = clk.get_query_results(q)
for scripts in tables.iterrows():
    script_str = scripts[1].script.split(';')
    for line in script_str:
        line = line.strip()
        if line != '':
            print(line)
            try:
                clk.get_query_results(line)
            except:
                print(str(sys.exc_info()[1]))
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
        
table_dict = {
'`EXPORT_CLICK.installs by month`': '"STG_CLICK_WEBAPP"."INSTALLS_BY_MONTH"',
'`EXPORT_CLICK.web_mau`': '"STG_CLICK_WEBAPP"."WEB_MAU"',
'`EXPORT_CLICK.installs by date`':'"STG_CLICK_WEBAPP"."INSTALLS_BY_DATE"',
'`EXPORT_CLICK.app_mau`':'"STG_CLICK_WEBAPP"."APP_MAU"'
}

for table in tables:
    try:
        print(table_dict[table])

        q = f'''SELECT * FROM {table}'''
        engine = create_engine(keys)
        table_df = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
        if 'date' not in table_df.columns:
            table_df['date'] = datetime.datetime.now().date()
        click_name = 'export_pg.'+table.replace(' ','_').replace('EXPORT_CLICK.','BQ_').replace('`','')
        q = f'''
            TRUNCATE TABLE
            {table_dict[table]} '''
        engine.execute(q)

        upload_multipart(click_name,table_df)
        engine.dispose()
    except:
        print(str(sys.exc_info()[1]))
        
internal_table_dict = {
    'export_pg.NB_GAINS': {'resulter': '"STG_CLICK_WEBAPP"."NB_GAINS"',
                        'source': 'export_pg.NB_GAINS_VIEW'}
}

    
for table in internal_table_dict:
    
    print(internal_table_dict[table])
    
    engine = create_engine(keys)
    
    if 'date' not in table_df.columns:
        table_df['date'] = datetime.datetime.now().date()
    q = f'''
        TRUNCATE TABLE
        {internal_table_dict[table]['resulter']} '''
    engine.execute(q)
    print(q)
    q = f'''INSERT INTO {table} SELECT * FROM {internal_table_dict[table]['source']};'''
    clk.get_query_results(q)
    print(q)
    engine.dispose()