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
import re
import sys

def date_pairs(date1, date2, step= 1):
    pairs= []
    while date2 >= date1:
        prev_date = date2 - datetime.timedelta(days=step-1) if date2 - datetime.timedelta(days=step) >= date1 else date1
        pair = [str(prev_date), str(date2)]   
        date2 -= datetime.timedelta(days=step)
        pairs.append(pair)
    pairs.reverse()
    return pairs

# def decodes(s):
#     import urllib
#     s  = s.replace('%25','%')
#     s2 = urllib.parse.unquote(s)
#     if '%' in s2:
#         s2 = s2.replace('25','')
#         s2 = urllib.parse.unquote(s2)
#     return s2

# def unix_preprocessing(date):
#     if ":" in date:
#         text = date.lower()
#         text = re.sub('[a-zA-Z]+', ' ', text)
#         text = re.sub('\+[0-9]+', ' ', text)
#         text = re.sub('\.[0-9]+', ' ', text)
#         text = re.sub('\ :[0-9]+', ' ', text)
#         text = re.sub('\ -[0-9]+', ' ', text)
#         text = re.sub('\ :[0-9]+', '', text)
#         return text.strip()        
#     else:
#         unix_timestamp = float(date)
#         a = (datetime.datetime.utcfromtimestamp(unix_timestamp).strftime('%Y-%m-%d %H:%M:%S'))
#     return a 

# key_path = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\m2-main-cd9ed0b4e222.json'

key_path = '/home/web_analytics/m2-main-cd9ed0b4e222.json'

gbq_credential = service_account.Credentials.from_service_account_file(key_path,)

q = """SELECT MAX(dateHourMinute) as date FROM `m2-main.UA_REPORTS.USERS_DT`"""
last_dt = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
start = last_dt['date'][0].date() + datetime.timedelta(days=1)

ga_conc = ga_connect('208464364')
tries = 200
while start < datetime.datetime.today().date():
    
    tries-=1
    
    try:
        dates_couples = date_pairs(start,start)
        
        print(dates_couples)
        
        params =  {'dimetions': [
                                 {'name': 'ga:dateHourMinute'},
                                 {'name': 'ga:dimension1'},
                                 {'name': 'ga:dimension2'},
                                 {'name': 'ga:dimension3'}
                                 ],
                    'metrics':   [
                                 {'expression': 'ga:users'}
                                 ]}
        all_traf_new = ga_conc.report_pd(dates_couples,params)
                   
        all_traf_new['dateHourMinute'] = all_traf_new['dateHourMinute'].apply(lambda x: datetime.datetime.strptime(x,"%Y%m%d%H%M"))
        all_traf_new = all_traf_new.drop(columns = ['users'])
        all_traf_new.to_gbq(f'UA_REPORTS.USERS_DT', project_id='m2-main',chunksize=20000, if_exists='append', credentials=gbq_credential)
        
        start += datetime.timedelta(days=1)
        time.sleep(10)
    except:
        print(str(sys.exc_info()[1]))
        time.sleep(30)
        start += datetime.timedelta(days=1)