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

# key_path = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\m2-main-cd9ed0b4e222.json'
key_path = '/home/kalmukds/m2-main-cd9ed0b4e222.json'
gbq_credential = service_account.Credentials.from_service_account_file(key_path,)

# DATAFRAME TRANSFORM
def decodes(s):
    
    
    s  = s.replace('%25','%')
    s2 = urllib.parse.unquote(s)
    if '%' in s2:
        s2 = s2.replace('25','')
        s2 = urllib.parse.unquote(s2)
        
    return s2

def date_pairs(date1, date2, step= 1):
    
    pairs= []
    while date2 >= date1:
        prev_date = date2 - datetime.timedelta(days=step-1) if date2 - datetime.timedelta(days=step) >= date1 else date1
        pair = [str(prev_date), str(date2)]   
        date2 -= datetime.timedelta(days=step)
        pairs.append(pair)
    pairs.reverse()
    
    return pairs

def get_start_date(table):
    
    clk  = clickhouse_pandas('ga')

    q = f"""SELECT  MAX(date) as date FROM {table['name']}"""
    last_dt = clk.get_query_results(q)
    start = last_dt['date'][0] + datetime.timedelta(days=1)
    
    return start


def all_traffic_transform(all_traf_new):
    
    all_traf_new['source'] = all_traf_new['sourcemedium'].apply(lambda x : x.split(' / ')[0])
    all_traf_new['medium'] = all_traf_new['sourcemedium'].apply(lambda x : x.split(' / ')[1])
    all_traf_new = all_traf_new.drop(columns = ['sourcemedium', 'users'])
    all_traf_new['dateHourMinute'] = all_traf_new['dateHourMinute'].apply(lambda x: datetime.datetime.strptime(x,"%Y%m%d%H%M"))
    all_traf_new['keyword'] = all_traf_new['keyword'].apply(decodes)
    all_traf_new['date'] = all_traf_new['dateHourMinute'].apply(lambda x : x.date())
    
    return all_traf_new

def all_users_transform(all_traf_new):
    
    all_traf_new['dateHourMinute'] = all_traf_new['dateHourMinute'].apply(lambda x: datetime.datetime.strptime(x,"%Y%m%d%H%M"))
    all_traf_new['date'] = all_traf_new['dateHourMinute'].apply(lambda x : x.date())
    all_traf_new = all_traf_new.drop(columns = ['users'])
    
    return all_traf_new

def all_event_transform(all_traf_new):
    
    all_traf_new['dateHourMinute'] = all_traf_new['dateHourMinute'].apply(lambda x: datetime.datetime.strptime(x,"%Y%m%d%H%M"))
    all_traf_new['date'] = all_traf_new['dateHourMinute'].apply(lambda x : x.date())
    return all_traf_new

def all_users_legasy(all_traf_new):
    
    
    return all_traf_new

def all_session_time(all_traf_new):
    
    all_traf_new['bounces'] = all_traf_new['bounces'].astype(int)
    all_traf_new['sessionDuration'] = all_traf_new['sessionDuration'].astype(float)
    all_traf_new['hits'] = all_traf_new['hits'].astype(int)
    all_traf_new['date']  = all_traf_new['date'].astype(str)
    all_traf_new['date'] = all_traf_new['date'].apply(lambda x : datetime.datetime.strptime(x,"%Y-%m-%d"))
    
    return all_traf_new


def all_users_plus(all_traf_new):

    all_traf_new['date']  = all_traf_new['date'].astype(str)
    all_traf_new['date'] = all_traf_new['date'].apply(lambda x : datetime.datetime.strptime(x,"%Y-%m-%d"))
    
    return all_traf_new

def all_traffic_transform_bq(all_traf_new):
    
    all_traf_new['source'] = all_traf_new['sourcemedium'].apply(lambda x : x.split(' / ')[0])
    all_traf_new['medium'] = all_traf_new['sourcemedium'].apply(lambda x : x.split(' / ')[1])
    all_traf_new = all_traf_new.drop(columns = ['sourcemedium', 'users'])
    all_traf_new['dateHourMinute'] = all_traf_new['dateHourMinute'].apply(lambda x: datetime.datetime.strptime(x,"%Y%m%d%H%M"))
    all_traf_new['keyword'] = all_traf_new['keyword'].apply(decodes)
    
    return all_traf_new

def all_users_transform_bq(all_traf_new):
    all_traf_new['dateHourMinute'] = all_traf_new['dateHourMinute'].apply(lambda x: datetime.datetime.strptime(x,"%Y%m%d%H%M"))
    all_traf_new = all_traf_new.drop(columns = ['users'])
    
    return all_traf_new

def all_event_transform_bq(all_traf_new):
    
    all_traf_new['dateHourMinute'] = all_traf_new['dateHourMinute'].apply(lambda x: datetime.datetime.strptime(x,"%Y%m%d%H%M"))
    
    return all_traf_new

tables = [
    {
     'name': 'ga.UA_TRAFIC_BIG',
     'bq_name': 'UA_REPORTS.UA_TRAFIC_BIG',
     'funcs' : all_traffic_transform,
     'funcs_bq' : all_traffic_transform_bq,
     'date_partition' : 'date',
     'params': {'dimetions': [
                             {'name': 'ga:dateHourMinute'},
                             {'name': 'ga:landingpagepath'},
                             {'name': 'ga:dimension1'},
                             {'name': 'ga:dimension4'},
                             {'name': 'ga:sourcemedium'},                         
                             {'name': 'ga:campaign'},
                             {'name': 'ga:adContent'},
                             {'name': 'ga:keyword'},
                             {'name': 'ga:deviceCategory'}      
                             ],
                'metrics':   [
                             {'expression': 'ga:users'}
                             ],
                'filters': ''}},
    {'name': 'ga.USERS_DT',
     'bq_name': 'UA_REPORTS.USERS_DT',
     'funcs' : all_users_transform,
     'funcs_bq' : all_users_transform_bq,
     'date_partition' : 'date',
     'params': {'dimetions': [
                             {'name': 'ga:dateHourMinute'},
                             {'name': 'ga:dimension1'},
                             {'name': 'ga:dimension2'},
                             {'name': 'ga:dimension3'}
                             ],
                'metrics':   [
                             {'expression': 'ga:users'}
                             ],
                'filters': ''}},
    
    {'name': 'ga.RAW_EVENTS',
     'bq_name': 'UA_REPORTS.RAW_EVENTS',
     'funcs' : all_event_transform,
     'funcs_bq' : all_event_transform_bq,
     'date_partition' : 'date',
     'params': {'dimetions': [
                             {'name': 'ga:dateHourMinute'},
                             {'name': 'ga:dimension4'},
                             {'name': 'ga:pagepath'},
                             {'name': 'ga:eventlabel'},            
                             {'name': 'ga:eventAction'},
                             {'name': 'ga:eventCategory'}
                             ],
                'metrics':   [
                             {'expression': 'ga:users'},
                             {'expression': 'ga:totalEvents'},
                             {'expression': 'ga:uniqueEvents'}
                             ],
                'filters': 'ga:eventlabel!~View|^(Show)$'}},
    
    {'name': 'ga.PAGE_VIEWS',
     'bq_name': 'UA_REPORTS.PAGE_VIEWS',
     'funcs' : all_event_transform,
     'funcs_bq' : all_event_transform_bq,
     'date_partition' : 'date',
     'params': {'dimetions': [
                             {'name': 'ga:dateHourMinute'},
                             {'name': 'ga:dimension4'},
                             {'name': 'ga:pagepath'}
                             ],
                'metrics':   [
                             {'expression': 'ga:pageviews'}
                             ],
                'filters': ''}}
]
ga_conc = ga_connect('208464364')


logger_all = clickhouse_logger('UA_REPORTS - ALL_TABLES')
ROWS_ALL_UPDATED = 0
try:
    
    for table in tables:
        
        logger_table = clickhouse_logger(table['name'])
        
        try:
            start = get_start_date(table)

            end =  datetime.datetime.today().date() - datetime.timedelta(days=1)

            dates_couples = date_pairs(start, end)

            # Логируем отчётные периоды отчёты
            logger_table.add_data_start(start)
            logger_table.add_data_end(end)

            params = table['params']
            
            for dates in dates_couples:
                
                dates_couple_1 = [dates]

                UA_report = ga_conc.report_pd(dates_couple_1,params)

                logger_table.add_rows_recieved(len(UA_report))

                UA_report_click = table['funcs'](UA_report)

    #             # Записываем полученные данные

                clk  = clickhouse_pandas('ga')
                clk.insert(UA_report_click, table['name'])
                
                UA_report_bq = table['funcs_bq'](UA_report)
                UA_report_bq.to_gbq(table['bq_name'], project_id='m2-main',chunksize=20000, if_exists='append', credentials=gbq_credential)
            
            # Логируем полученые данные

            logger_table.add_rows_updated(len(UA_report))
            ROWS_ALL_UPDATED += len(UA_report)

            logger_table.no_errors_found()
        except:
            logger_table.errors_found(str(sys.exc_info()[1]))
            
    logger_all.add_rows_updated(ROWS_ALL_UPDATED)
    logger_all.no_errors_found()
    
except:
    
    logger_all.errors_found(str(sys.exc_info()[1]))