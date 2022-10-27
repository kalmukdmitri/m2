# from clickhouse_py  import clickhouse_pandas, clickhouse_logger
# from datetime import datetime
# from ga_connector_click import ga_connect
# from google.oauth2 import service_account
# from googleapiclient.discovery import build
# from clickhouse_driver import Client
# from numpy import dtype
# from oauth2client.service_account import ServiceAccountCredentials
# import csv
# import datetime
# import json
# import numpy
# import os
# import pandas
# import requests
# import string
# import sys
# import pandas_gbq
# import re
# import time


# def date_pairs(date1, date2, step= 1):
#     pairs= []
#     while date2 >= date1:
#         prev_date = date2 - datetime.timedelta(days=step-1) if date2 - datetime.timedelta(days=step) >= date1 else date1
#         pair = [str(prev_date), str(date2)]   
#         date2 -= datetime.timedelta(days=step)
#         pairs.append(pair)
#     pairs.reverse()
#     return pairs

# key_path = '/home/kalmukds/m2-main-cd9ed0b4e222.json'

# gbq_credential = service_account.Credentials.from_service_account_file(key_path,)

# # q = """SELECT MAX(date) as date FROM `m2-main.UA_REPORTS.USERS_V2`"""
# # last_dt = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)

# # start = last_dt['date'][0].date() + datetime.timedelta(days=1)

# start = datetime.date(2021, 1, 1)

# ga_conc = ga_connect('208464364')
# tries = 200
# while start < datetime.datetime.today().date():
    
#     tries-=1
    
#     try:
#         dates_couples = date_pairs(start,start)
        
#         print(dates_couples)
        
#         params =  {'dimetions': [
#                              {'name': 'ga:dimension4'},
#                              {'name': 'ga:dimension2'},
#                              ],
#                 'metrics':   [
#                              {'expression': 'ga:users'}
#                              ],
#                 'filters': ''}
#         all_traf_new = ga_conc.report_pd(dates_couples,params)
                   
#         all_traf_new = all_traf_new.drop(columns = ['users'])
#         all_traf_new['date'] = len(all_traf_new) * [start]
#         all_traf_new['date'] = all_traf_new['date'].apply(lambda x: pandas.Timestamp(x))        
        
#         all_traf_new.to_gbq(f'UA_REPORTS.USERS_V2', project_id='m2-main',chunksize=20000, if_exists='append', credentials=gbq_credential)
        
#         start += datetime.timedelta(days=1)
#         time.sleep(10)
#     except:
#         print(str(sys.exc_info()[1]))
#         time.sleep(30)
#         start += datetime.timedelta(days=1)

        
        
from ga_connector_click import ga_connect
# from google.oauth2 import service_account
# from googleapiclient.discovery import build
# from oauth2client.service_account import ServiceAccountCredentials
# import bigquery_logger
import datetime
# import gspread
import json
import os
import pandas
# import pandas_gbq
import sys
from clickhouse_py  import clickhouse_pandas, clickhouse_logger
params =  {'dimetions': [
                     {'name': 'ga:dateHourMinute'},
                     {'name': 'ga:dimension4'},
                     {'name': 'ga:pagepath'},
                     {'name': 'ga:hostname'}
                     ],
        'metrics':   [
                     {'expression': 'ga:timeOnPage'},
                     {'expression': 'ga:pageviews'}

                     ],
         'filters': ''} 

def date_pairs(date1, date2, step= 1):
    pairs= []
    while date2 >= date1:
        prev_date = date2 - datetime.timedelta(days=step-1) if date2 - datetime.timedelta(days=step) >= date1 else date1
        pair = [str(prev_date), str(date2)]   
        date2 -= datetime.timedelta(days=step)
        pairs.append(pair)
    pairs.reverse()
    return pairs
clk  = clickhouse_pandas('ga')
q = 'SELECT MAX(date) as date FROM ga.PAGE_TIME'
last_dt = clk.get_query_results(q)['date'][0] 

ga_conc = ga_connect('208464364')
start = str(last_dt + datetime.timedelta(days=1))


tries = 200
while start < datetime.datetime.today().date():
    
    tries-=1
    UA_report2 = ga_conc.report_pd([[start,start]], params)
    UA_report2['dateHourMinute'] = UA_report2['dateHourMinute'].apply(lambda x: datetime.datetime.strptime(x,"%Y%m%d%H%M"))
    UA_report2['date'] = UA_report2['dateHourMinute'].apply(lambda x : x.date())
    clk.insert(UA_report2, 'ga.PAGE_TIME')