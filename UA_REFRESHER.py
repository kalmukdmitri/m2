from ga_connector import ga_connect
from google.oauth2 import service_account
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import datetime
import gspread
import json
import os
import pandas
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

key_path = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\m2-main-cd9ed0b4e222.json'
gbq_credential = service_account.Credentials.from_service_account_file(key_path,)
q = """SELECT  MAX(DATE) as date FROM `m2-main.UA_REPORTS.UA_TRAFIC_FULL` """
last_dt = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
start = datetime.datetime.strptime(last_dt['date'][0],"%Y-%m-%d" ).date() + datetime.timedelta(days=1)
end =  datetime.datetime.today().date() - datetime.timedelta(days=1)
dates_couples = date_pairs(start, end)

ga_conc = ga_connect('208464364')
params = {'dimetions':  [{'name': 'ga:date'},
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
        
        'filters': ''
        }
all_traf_new = ga_conc.report_pd(dates_couples,params)
all_traf_new['source'] = all_traf_new['sourcemedium'].apply(lambda x : x.split(' / ')[0])
all_traf_new['medium'] = all_traf_new['sourcemedium'].apply(lambda x : x.split(' / ')[1])
all_traf_new = all_traf_new.drop(columns = ['sourcemedium', 'users'])
all_traf_new['date'] = all_traf_new['date'].astype(str)
all_traf_new.to_gbq(f'UA_REPORTS.UA_TRAFIC_FULL', project_id='m2-main',chunksize=10000, if_exists='append', credentials=gbq_credential)

q = """SELECT  MAX(DATE) as date FROM `m2-main.UA_REPORTS.UA_ALL_CLOPS` """
last_dt = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
start = datetime.datetime.strptime(last_dt['date'][0],"%Y-%m-%d" ).date() + datetime.timedelta(days=1)
end =  datetime.datetime.today().date() - datetime.timedelta(days=1)
dates_couples = date_pairs(start, end)
ga_conc = ga_connect('208464364')


filtr = 'ga:eventlabel=~Phone'

params = {'dimetions':  [{'name': 'ga:date'},
                         {'name': 'ga:dimension1'},
                         {'name': 'ga:dimension4'},
                         {'name': 'ga:pagepath'},
                         {'name': 'ga:eventlabel'},            
                         {'name': 'ga:eventAction'},
                         {'name': 'ga:eventCategory'}
                        ],  
        'metrics':[{'expression': 'ga:users'},
                   {'expression': 'ga:totalEvents'},
                   {'expression': 'ga:uniqueEvents'}
                  ],
        
        'filters': filtr
        }


clops = ga_conc.report_pd(dates_couples,params)
clops['date'] = clops['date'].astype(str)
clops.to_gbq(f'UA_REPORTS.UA_ALL_CLOPS', project_id='m2-main',chunksize=20000, if_exists='append', credentials=gbq_credential)