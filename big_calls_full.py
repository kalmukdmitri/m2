import pandas_gbq
import requests
import datetime
import json
import pandas
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
import gspread
import string

key_path = '/home/web_analytics/m2-main-cd9ed0b4e222.json'

gbq_credential = service_account.Credentials.from_service_account_file(key_path,)
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
             'https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(key_path, SCOPES)

gc = gspread.authorize(credentials)
sh = gc.open_by_key("1DaZoAZjE_yg2pKyAxY_YqBT6hWXuS-elHWPFvzgANbQ")
wk = sh.worksheet('Лист1')
list_of_dicts = wk.get_all_records()
calls_g_c = pandas.DataFrame(list_of_dicts)
calls_g_c = calls_g_c[calls_g_c['date_time'] !=  '']
calls_g_c = calls_g_c[calls_g_c['date_time'] !=  '-']
calls_g_c = calls_g_c.drop(columns = ['empt1', 'empt2', 'empt3','empt4'])
calls_g_c['date_time'] = calls_g_c['date_time'].apply(lambda x: x.replace('   ',' '))
calls_g_c['partner_source'] = calls_g_c['partner_source'].apply(lambda x: x if x not in ('','#N/A','#REF!') else '-')
calls_g_c['sold_sum'] = calls_g_c['sold_sum'].apply(lambda x: 0 if '-' == x else x)
calls_g_c['sold_sum'] = calls_g_c['sold_sum'].apply(lambda x: 0 if x == '' else x)
for i in calls_g_c.columns:
    calls_g_c[i] = calls_g_c[i].astype(str)
calls_g_c['sold_sum'] = calls_g_c['sold_sum'].astype(int)
calls_g_c['date_time'] = calls_g_c['date_time'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d %H:%M:%S" ))
calls_g_c.to_gbq(f'sheets.NB_ALL_CALLS', project_id='m2-main', if_exists='replace', credentials=gbq_credential)