from ga_connector import ga_connect
from google.oauth2 import service_account
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import datetime
import gspread
import json
import os
import pandas
import time
import pandas_gbq

def date_pairs(date1, date2, step= 1):
    pairs= []
    while date2 >= date1:
        prev_date = date2 - datetime.timedelta(days=step-1) if date2 - datetime.timedelta(days=step) >= date1 else date1
        pair = [str(prev_date), str(date2)]   
        date2 -= datetime.timedelta(days=step)
        pairs.append(pair)
    pairs.reverse()
    return pairs

key_path = '/home/web_analytics/m2-main-cd9ed0b4e222.json'
gbq_credential = service_account.Credentials.from_service_account_file(key_path,)

q = """SELECT  MAX(DATE) as date FROM `m2-main.UA_REPORTS.USERS` """
last_dt = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
start = datetime.datetime.strptime(last_dt['date'][0],"%Y-%m-%d" ).date() + datetime.timedelta(days=1)
while start < datetime.datetime.today().date():
    dates_couples = date_pairs(start,start)
    start += datetime.timedelta(days=1)
    print(dates_couples)

    ga_conc = ga_connect('208464364')
    filtr = ''
    params = {'dimetions':  [{'name': 'ga:date'},
                             {'name': 'ga:dimension1'},
                             {'name': 'ga:dimension2'},
                             {'name': 'ga:dimension3'}],
            'metrics' : [{'expression': 'ga:users'}],
            'filters' : filtr}

    USERS = ga_conc.report_pd(dates_couples,params)
    USERS.to_gbq(f'UA_REPORTS.USERS', project_id='m2-main',chunksize=20000, if_exists='append', credentials=gbq_credential)
    time.sleep(3)