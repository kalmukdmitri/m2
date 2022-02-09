from ga_connector import ga_connect
from google.oauth2 import service_account
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import bigquery_logger
import datetime
import gspread
import json
import os
import pandas
import pandas_gbq
import sys

# GENERAL FUNC

def date_pairs(date1, date2, step= 1):
    pairs= []
    while date2 >= date1:
        prev_date = date2 - datetime.timedelta(days=step-1) if date2 - datetime.timedelta(days=step) >= date1 else date1
        pair = [str(prev_date), str(date2)]   
        date2 -= datetime.timedelta(days=step)
        pairs.append(pair)
    pairs.reverse()
    return pairs

def decodes(s):
    import urllib
    s  = s.replace('%25','%')
    s2 = urllib.parse.unquote(s)
    if '%' in s2:
        s2 = s2.replace('25','')
        s2 = urllib.parse.unquote(s2)
    return s2

key_path = '/home/web_analytics/m2-main-cd9ed0b4e222.json'
gbq_credential = service_account.Credentials.from_service_account_file(key_path,)

def get_start_date(tables):
    
    if tables['date_partition'] == 'dateHourMinute':
        q = f"""SELECT  MAX(dateHourMinute) as date FROM `m2-main.{table['name']}` """
        last_dt = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)  
        start = last_dt['date'][0].date() + datetime.timedelta(days=1)
    elif tables['date_partition'] == 'date':
        q = f"""SELECT  MAX(DATE) as date FROM `m2-main.{table['name']}` """
        last_dt = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
        if type(last_dt['date'][0]) == pandas._libs.tslibs.timestamps.Timestamp:
            last_dt = str(last_dt['date'][0])[:10]
        else:
            last_dt = last_dt['date'][0]
        start = datetime.datetime.strptime(last_dt,"%Y-%m-%d" ).date() + datetime.timedelta(days=1)
    return start


# DATAFRAME TRANSFORM
def all_traffic_transform(all_traf_new):
    
    all_traf_new['source'] = all_traf_new['sourcemedium'].apply(lambda x : x.split(' / ')[0])
    all_traf_new['medium'] = all_traf_new['sourcemedium'].apply(lambda x : x.split(' / ')[1])
    all_traf_new = all_traf_new.drop(columns = ['sourcemedium', 'users'])
    all_traf_new['dateHourMinute'] = all_traf_new['dateHourMinute'].apply(lambda x: datetime.datetime.strptime(x,"%Y%m%d%H%M"))
    all_traf_new['keyword'] = all_traf_new['keyword'].apply(decodes)
    
    return all_traf_new

def all_users_transform(all_traf_new):
    
    return all_traf_new

def all_event_transform(all_traf_new):
    
    all_traf_new['dateHourMinute'] = all_traf_new['dateHourMinute'].apply(lambda x: datetime.datetime.strptime(x,"%Y%m%d%H%M"))
    
    return all_traf_new

def all_session_time(all_traf_new):
    
    all_traf_new['bounces'] = all_traf_new['bounces'].astype(int)
    all_traf_new['sessionDuration'] = all_traf_new['sessionDuration'].astype(float)
    all_traf_new['hits'] = all_traf_new['hits'].astype(int)
    all_traf_new['date']  = all_traf_new['date'].astype(str)
    all_traf_new['date'] = all_traf_new['date'].apply(lambda x : datetime.datetime.strptime(x,"%Y-%m-%d"))
    
    return all_traf_new

tables = [
    {'name': 'UA_REPORTS.UA_TRAFIC_BIG',
     'funcs' : all_traffic_transform,
     'date_partition' : 'dateHourMinute',
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
    {'name': 'UA_REPORTS.USERS',
     'funcs' : all_users_transform,
     'date_partition' : 'date',
     'params': {'dimetions': [
                             {'name': 'ga:date'},
                             {'name': 'ga:dimension1'},
                             {'name': 'ga:dimension2'},
                             {'name': 'ga:dimension3'}
                             ],
                'metrics':   [
                             {'expression': 'ga:users'}
                             ],
                'filters': ''}},
    
    {'name': 'UA_REPORTS.RAW_EVENTS',
     'funcs' : all_event_transform,
     'date_partition' : 'dateHourMinute',
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
    
    {'name': 'UA_REPORTS.PAGE_VIEWS',
     'funcs' : all_event_transform,
     'date_partition' : 'dateHourMinute',
     'params': {'dimetions': [
                             {'name': 'ga:dateHourMinute'},
                             {'name': 'ga:dimension4'},
                             {'name': 'ga:pagepath'}
                             ],
                'metrics':   [
                             {'expression': 'ga:pageviews'}
                             ],
                'filters': ''}},
    {'name': 'UA_REPORTS.VISIT_QUALITY',
     'funcs' : all_session_time,
     'date_partition' : 'date',
     'params': {'dimetions': [
                              {'name': 'ga:date'},
                              {'name': 'ga:dimension4'},
                              {'name': 'ga:dimension1'},
                              ],
                'metrics':   [
                              {'expression': 'ga:bounces'},
                              {'expression': 'ga:sessionDuration'},
                              {'expression': 'ga:hits'}
                              ],
                'filters': ''}}
]
ga_conc = ga_connect('208464364')
table_log_BIG = bigquery_logger.bq_logger('UA_REPORTS - ALL_TABLES')
ROWS_ALL_UPDATED = 0

try:
    for table in tables:

        table_log = bigquery_logger.bq_logger(table['name'])

        try:

            start = get_start_date(table)
            end =  datetime.datetime.today().date() - datetime.timedelta(days=1)

            dates_couples = date_pairs(start, end)

            # Логируем отчётные периоды отчёты
            table_log.add_data_start(str(start))
            table_log.add_data_end(str(end))

            params = table['params']

            UA_report = ga_conc.report_pd(dates_couples,params)
            
            table_log.add_rows_recieved(len(UA_report))

            # Логируем полученые данные
            UA_report = table['funcs'](UA_report) 
            UA_report.to_gbq(table['name'], project_id='m2-main',chunksize=20000, if_exists='append', credentials=gbq_credential)

            table_log.add_rows_updated(len(UA_report))
            ROWS_ALL_UPDATED += len(UA_report)

            table_log.no_errors_found()
            
        except:
            
            table_log.errors_found(str(sys.exc_info()[1]))
            
except:
    
    table_log_BIG.errors_found(str(sys.exc_info()[1]))
