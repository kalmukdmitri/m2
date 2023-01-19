from clickhouse_py  import clickhouse_pandas, clickhouse_logger
from datetime import datetime
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
import time

# key_path = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\m2-main-cd9ed0b4e222.json'
key_path = '/home/kalmukds/m2-main-cd9ed0b4e222.json'
# gbq_credential = service_account.Credentials.from_service_account_file(key_path,)
clk  = clickhouse_pandas('ga')

key_path_pg = '/home/kalmukds/pg_keys_special.json'
# key_path_pg = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\pg_keys_special.json'
f = open(key_path_pg, "r")
key_other = f.read()
keys = json.loads(key_other)['pg']
# print(keys)

# q= '''
# SELECT
#   table_name
# FROM
#   EXPORT_CLICK.INFORMATION_SCHEMA.VIEWS;'''
# export_tables = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)  
# tables = ['`EXPORT_CLICK.'+f'{i}`' for i in export_tables['table_name']]

# engine = create_engine(keys).execution_options(isolation_level="AUTOCOMMIT")


def upload_multipart(table_name, df):
    
    dates_list = list(set(df['date']))
    dates_list.sort()
    
    for i in range(0,len(dates_list), 10):
        
        date_block = dates_list[i:i+10]
        regs_date = df[df['date'].apply(lambda x: x in date_block)]
        regs_date = regs_date.reset_index(drop=True)
        clk.insert(regs_date, table_name)
        
# table_dict = {
# '`EXPORT_CLICK.installs by month`': '"STG_CLICK_WEBAPP"."INSTALLS_BY_MONTH"',
# '`EXPORT_CLICK.web_mau`': '"STG_CLICK_WEBAPP"."WEB_MAU"',
# '`EXPORT_CLICK.installs by date`':'"STG_CLICK_WEBAPP"."INSTALLS_BY_DATE"',
# '`EXPORT_CLICK.app_mau`':'"STG_CLICK_WEBAPP"."APP_MAU"'}

# for table in tables:
# #     try:
#     print(table_dict[table])
#     print(keys)
#     try:
#         q = f'''SELECT * FROM {table}'''
#         print(q)
#         table_df = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
#         if 'date' not in table_df.columns:
#             table_df['date'] = datetime.datetime.now().date()
#         click_name = 'export_pg.'+table.replace(' ','_').replace('EXPORT_CLICK.','BQ_').replace('`','')

#         with engine.connect() as connection:

#             q = f'''
#             TRUNCATE TABLE {table_dict[table]} '''
#             print(q)
#             result = connection.execute(q)
#             time.sleep(3)
#             connection.close()

#         upload_multipart(click_name,table_df)
#     except:
#         print(str(sys.exc_info()[1]))
# engine.dispose()  
#     except:
#         print(str(sys.exc_info()[1]))
        
internal_table_dict = {
    'export_pg.NB_GAINS': {'resulter': '"STG_CLICK_WEBAPP"."NB_GAINS"',
                           'source': 'export_pg.NB_GAINS_VIEW'},
    
    'export_pg.UP_VAS_TABLE': {'resulter': '"STG_CLICK_WEBAPP"."UP_VAS_TABLE"',
                           'source': 'external.UP_VAS_TABLE'},
    
    'export_pg.BQ_app_mau': {'resulter': '"STG_CLICK_WEBAPP"."APP_MAU"',
                           'source': 'external.app_mau_view'},
  
    'export_pg.BQ_installs_by_date': {'resulter': '"STG_CLICK_WEBAPP"."INSTALLS_BY_DATE"',
                           'source': 'external.installs_by_date_view'},
    
    'export_pg.BQ_installs_by_month': {'resulter': '"STG_CLICK_WEBAPP"."INSTALLS_BY_MONTH"',
                           'source': 'external.installs_by_month'},
    
    'export_pg.BQ_web_mau': {'resulter': '"STG_CLICK_WEBAPP"."WEB_MAU"',
                           'source': 'external.web_mau'}git
}

engine = create_engine(keys).execution_options(isolation_level="AUTOCOMMIT")
clk  = clickhouse_pandas('ga')
for table in internal_table_dict:
    
    print(internal_table_dict[table])
    try:    
        
        with engine.connect() as connection:

            q = f'''
            TRUNCATE TABLE {internal_table_dict[table]['resulter']} '''
            print(q)
            result = connection.execute(q)
            time.sleep(3)
            connection.close()

        q = f'''INSERT INTO {table} SELECT * FROM {internal_table_dict[table]['source']};'''
        print(q)
        clk.get_query_results(q)
    except:
        print(str(sys.exc_info()[1]))
    
engine.dispose()    
