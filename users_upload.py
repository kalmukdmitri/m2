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
q = """SELECT  MAX(dateHourMinute) as date FROM `m2-main.UA_REPORTS.UA_TRAFIC_BIG` """
last_dt = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)

start = last_dt['date'][0].date() + datetime.timedelta(days=1)
ga_conc = ga_connect('208464364')
while start < datetime.datetime.today().date():
    dates_couples = date_pairs(start,start)
    start += datetime.timedelta(days=1)
    print(dates_couples)

    filtr = ''
    
    params = {'dimetions':  [{'name': 'ga:dateHourMinute'},
                             {'name': 'ga:landingpagepath'},
                             {'name': 'ga:dimension1'},
                             {'name': 'ga:dimension4'},
                             {'name': 'ga:sourcemedium'},                         
                             {'name': 'ga:campaign'},
                             {'name': 'ga:adContent'},
                             {'name': 'ga:keyword'},
                             {'name': 'ga:deviceCategory'}      

                                    ],
                    'metrics':[{'expression': 'ga:users'}
                              ],

                    'filters': ''}

    all_traf_new = ga_conc.report_pd(starts,params)
    
    all_traf_new['dateHourMinute'] = all_traf_new['dateHourMinute'].apply(lambda x: datetime.datetime.strptime(x,"%Y%m%d%H%M"))
    all_traf_new['source'] = all_traf_new['sourcemedium'].apply(lambda x : x.split(' / ')[0])
    all_traf_new['medium'] = all_traf_new['sourcemedium'].apply(lambda x : x.split(' / ')[1])
    all_traf_new['keyword'] = all_traf_new['keyword'].apply(decodes)
    all_traf_new = all_traf_new.drop(columns = ['sourcemedium', 'users'])
    
    all_traf_new.to_gbq(f'UA_REPORTS.UA_TRAFIC_BIG', project_id='m2-main',chunksize=20000, if_exists='append', credentials=gbq_credential)
    time.sleep(10)