from google.oauth2 import service_account
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

gbq_credential = service_account.Credentials.from_service_account_file(key_path,)

tables = [
    'UA_TRAFIC_BIG',
    'USERS_DT',
    'RAW_EVENTS',
    'PAGE_VIEWS',
]
clk  = clickhouse_pandas('ga')
for table in tables:
    q  = f'''
    SELECT MAX(date) as l_dt FROM ga.{table}
    where date < '2022-08-15'
    '''
    date = clk.get_query_results(q)['l_dt'][0]
    date = datetime.date(2020, 12, 31) if date == datetime.date(1970, 1, 1) else date
    date += datetime.timedelta(days=1)
    print(date)
    print(table)
    while date < datetime.date(2022, 8, 15):
        q  = f'''
        SELECT MAX(date) as l_dt FROM ga.{table}
        where date < '2022-08-15'
        '''
        last_date_ct = clk.get_query_results(q)['l_dt'][0]
        if last_date_ct == datetime.date(1970, 1, 1):
            q = f'SELECT min(date(dateHourMinute)) as date FROM `m2-main.UA_REPORTS.{table}`'
            first_dt = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
            last_date_ct = first_dt['date'][0].date()
        
        else:
            last_date_ct+= datetime.timedelta(days=1)
        print(last_date_ct)
        print(q)

        q = f"""SELECT * FROM `m2-main.UA_REPORTS.{table}`
        where date(dateHourMinute) = '{last_date_ct}' """
        
        data = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
        data['date'] = data['dateHourMinute'].apply(lambda x : x.date())
        data['dateHourMinute'] = data['dateHourMinute'].apply(lambda x : x.tz_localize(None))
        clk  = clickhouse_pandas('ga')
        clk.insert(data, f'ga.{table}')