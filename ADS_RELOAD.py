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

dicts_reload = {
    '' : ''
}
# key_path = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\m2-main-cd9ed0b4e222.json'
key_path = '/home/kalmukds/m2-main-cd9ed0b4e222.json'
gbq_credential = service_account.Credentials.from_service_account_file(key_path,)

print('started')

def upload_multipart(table_name, df):
    
    dates_list = list(set(df['date']))
    dates_list.sort()
    
    for i in range(0,len(dates_list), 10):
        
        date_block = dates_list[i:i+10]
        regs_date = df[df['date'].apply(lambda x: x in date_block)]
        regs_date = regs_date.reset_index(drop=True)
        clk.insert(regs_date, table_name)

clk  = clickhouse_pandas('external')


table_bq = 'TEST_MART.ADS_ID_KEYWORD'
table_click = 'external.ADS_ID_KEYWORD'

# days = datetime.datetime.today().date() - datetime.timedelta(days=30)

# q = f'''SELECT * FROM {table_bq} where Date >= '{days}' '''
# table_df_bq = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
# table_df_bq['date'] = table_df_bq['Date']
# table_df_bq = table_df_bq.drop(columns = ['Date'])
# clear_q = f"ALTER TABLE {table_click} DELETE WHERE date >= '{days}'; "
# clk.get_query_results(clear_q)
# upload_multipart('external.ADS', table_df_bq)

# print('ended')






clear_q = f"SELECT MAX(date) as dt FROM {table_click}; "
lst_record = clk.get_query_results(clear_q)['dt'][0]

while lst_record < datetime.date(2022,12,1):
    q = f'''SELECT * FROM {table_bq} where Date > '{lst_record}' and Date <= '{lst_record + datetime.timedelta(days=30)}'
    '''
    print(q)
    table_df_bq = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
    
    table_df_bq['date'] = table_df_bq['Date']
    table_df_bq = table_df_bq.fillna('None')
    table_df_bq = table_df_bq.drop(columns = ['Date'])
    

    upload_multipart(table_click, table_df_bq)