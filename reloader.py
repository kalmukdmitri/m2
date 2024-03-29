from google.oauth2 import service_account
from clickhouse_py  import clickhouse_pandas, clickhouse_logger
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import datetime
import json
import os
import pandas
import time
import pandas_gbq
import re
import sys

key_path = '/home/kalmukds/m2-main-cd9ed0b4e222.json'

# gbq_credential = service_account.Credentials.from_service_account_file(key_path,)

# tables = [
#     'UA_TRAFIC_BIG',
#     'USERS_DT',
#     'RAW_EVENTS',
#     'PAGE_VIEWS',
# ]
# clk  = clickhouse_pandas('ga')
# for table in tables:
#     q  = f'''
#     SELECT MAX(date) as l_dt FROM ga.{table}
#     where date < '2022-08-15'
#     '''
#     date = clk.get_query_results(q)['l_dt'][0]
#     date = datetime.date(2020, 12, 31) if date == datetime.date(1970, 1, 1) else date
#     date += datetime.timedelta(days=1)
#     print(date)
#     print(table)
#     cut_off = (datetime.datetime.today().date() - datetime.timedelta(days=11))
#     print(cut_off)
#     while date  < cut_off:
#         q  = f'''
#         SELECT MAX(date) as l_dt FROM ga.{table}
#         where date < '2022-08-15'
#         '''
#         last_date_ct = clk.get_query_results(q)['l_dt'][0]
#         if last_date_ct == datetime.date(1970, 1, 1):
#             q = f'SELECT min(date(dateHourMinute)) as date FROM `m2-main.UA_REPORTS.{table}`'
#             first_dt = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
#             last_date_ct = first_dt['date'][0].date()
        
#         else:
#             last_date_ct+= datetime.timedelta(days=1)
#         date = last_date_ct
#         print(date)
        

#         q = f"""SELECT * FROM `m2-main.UA_REPORTS.{table}`
#         where date(dateHourMinute) >= '{last_date_ct}' and
#         date(dateHourMinute) < DATE_ADD(DATE '{last_date_ct}', INTERVAL 10 DAY)
#         """
#         print(q)
        
#         data = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
#         data['date'] = data['dateHourMinute'].apply(lambda x : x.date())
#         data['dateHourMinute'] = data['dateHourMinute'].apply(lambda x : x.tz_localize(None))
#         clk  = clickhouse_pandas('ga')
#         clk.insert(data, f'ga.{table}')
        
gbq_credential = service_account.Credentials.from_service_account_file(key_path,)

tables = [
    'VISIT_QUALITY'
]
clk  = clickhouse_pandas('ga')
for table in tables:
    q  = f'''
    SELECT MAX(date) as l_dt FROM ga.{table}
    where date < '2022-11-15'
    '''
    date = clk.get_query_results(q)['l_dt'][0]
    date = datetime.date(2020, 12, 31) if date == datetime.date(1970, 1, 1) else date
    date += datetime.timedelta(days=1)
    print(date)
    print(table)
    cut_off = (datetime.datetime.today().date() - datetime.timedelta(days=1))
    print(cut_off)
    while date  < cut_off:
        q  = f'''
        SELECT MAX(date) as l_dt FROM ga.{table}
        where date < '2022-11-15'
        '''
        last_date_ct = clk.get_query_results(q)['l_dt'][0]
        if last_date_ct == datetime.date(1970, 1, 1):
            q = f'SELECT min(date(dateHourMinute)) as date FROM `m2-main.UA_REPORTS.{table}`'
            first_dt = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
            last_date_ct = first_dt['date'][0].date()
        
        else:
            last_date_ct+= datetime.timedelta(days=1)
        date = last_date_ct
        print(date)
        

        q = f"""SELECT * FROM `m2-main.UA_REPORTS.{table}`
        where DATE(date) >= '{last_date_ct}' and
        date(date) < DATE_ADD('{last_date_ct}', INTERVAL 10 DAY)"""
        print(q)
        data = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
#         data['date'] = data['dateHourMinute'].apply(lambda x : x.date())
#         data['dateHourMinute'] = data['dateHourMinute'].apply(lambda x : x.tz_localize(None))
        clk  = clickhouse_pandas('ga')
        clk.insert(data, f'ga.{table}')
