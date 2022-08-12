import pandas_gbq
import requests
from google.cloud import bigquery
import os
import datetime
import json
import pandas
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
import gspread
import string
from google.cloud import bigquery
import os

key_path = '/home/kalmukds/m2-main-cd9ed0b4e222.json'
key_path_extra = '/home/kalmukds/other_keys.json'
# key_path = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\m2-main-cd9ed0b4e222.json'
# key_path_extra = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\other_keys.json'


f = open(key_path_extra, "r")
key_other = f.read()

gbq_credential = service_account.Credentials.from_service_account_file(key_path,)
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
             'https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(key_path, SCOPES)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=key_path
gc = gspread.authorize(credentials)
keys = json.loads(key_other)['keys']['calltouch']


tokem = keys['tokem']
mod_id = keys['id']
cabinet = keys['cabinet']

def get_calls(start,end):
    start = start.strftime('%d/%m/%Y')
    end = end.strftime('%d/%m/%Y')
    q = f"http://api.calltouch.ru/calls-service/RestAPI/{cabinet}/calls-diary/calls?clientApiId={tokem}&dateFrom={start}&dateTo={end}&page=1&limit=10000"
    x = requests.get(q)
    g = json.loads(x.text)
    params = {}
    for i in g['records']:
        for j,k in i.items():
            if j in params:
                params[j].append(k)
            else:
                params[j] =[k]
    n = ['callId',
     'timestamp',
     'callerNumber',
     'phoneNumber',
     'callbackCall',
     'uniqueCall',
     'targetCall',
     'uniqTargetCall',
     'successful',
     'source',
     'medium',
     'keyword',
     'utmSource',
     'utmMedium',
     'utmCampaign',
     'utmContent',
     'utmTerm',
     'clientId',
     'url',
     'callUrl']
    df_call = pandas.DataFrame(params)
    
    drps = [ i for i in list(df_call.columns) if i not in n ]

    c_df_call = df_call.drop(columns= drps)
    c_df_call['date_time_msk'] = c_df_call['timestamp'].apply(lambda x: (datetime.datetime.utcfromtimestamp(x)+datetime.timedelta(hours=3)))
    return c_df_call


q  = '''
SELECT MAX(date_time_msk) as l_dt FROM kalmukds.CALLTOUCH_JOURNAL
'''
last_date_ct = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
start_date = last_date_ct['l_dt'][0].date() - datetime.timedelta(days=2)
end_date = datetime.datetime.today().date() - datetime.timedelta(days=1)

new_rows = get_calls(start_date,end_date)
new_rows['date'] = new_rows['date_time_msk'].apply(lambda x: x.date())

# Обновляем если есть новые данные

if len(new_rows) >0 :
    client = bigquery.Client()
    
    query_job = client.query(
        f"""
        DELETE
        FROM kalmukds.CALLTOUCH_JOURNAL
        WHERE date_time_msk >= '{start_date}'
        """
    )
    results = query_job.result()
    new_rows.to_gbq(f'kalmukds.CALLTOUCH_JOURNAL', project_id='m2-main', if_exists='append', credentials=gbq_credential)