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
sh = gc.open_by_key("1bMMb7RO-xHPxPe7fAwoypHtt__APaLF7E5UMHyFEXHU")
wk = sh.worksheet('nb_calls_bq')
list_of_dicts = wk.get_all_records()
calls_g_cite = pandas.DataFrame(list_of_dicts)
for i in calls_g_cite.columns:
    calls_g_cite[i] = calls_g_cite[i].astype(str)
calls_g_cite.to_gbq(f'sheets.nb_call_raw', project_id='m2-main', if_exists='replace', credentials=gbq_credential)