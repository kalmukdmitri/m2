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
key_path_extra = '/home/web_analytics/other_keys.json'

f = open(key_path_extra, "r")
key_other = f.read()

gbq_credential = service_account.Credentials.from_service_account_file(key_path,)
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
             'https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(key_path, SCOPES)

gc = gspread.authorize(credentials)
keys = json.loads(key_other)['keys']['calltouch']
print(keys)


def get_cmp(row):
    
    if row.utmCampaign != '<не указано>':
        return row.utmCampaign
    elif string.digits not in row.source and row.source != '(direct)':
        return row.source + '/' + row.medium
    else:
        return row.clientId


def get_ct_records(start,end,keys):
    if start == end:
        return pandas.DataFrame()
    start = start.strftime('%d/%m/%Y')
    end = end.strftime('%d/%m/%Y')
    tokem = keys['tokem']
    mod_id = keys['id']
    cabinet = keys['cabinet']

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
    df_call = pandas.DataFrame(params)
    n  = ['callerNumber','utmSource', 'utmMedium', 'utmCampaign','source', 'medium', 'keyword','clientId', 'timestamp']
    drps = [ i for i in list(df_call.columns) if i not in n ]

    c_df_call = df_call.drop(columns= drps)
    c_df_call_d = c_df_call[c_df_call['source'].apply(lambda x: 'пауза' not in x)]

    eds = []
    cols = list(c_df_call_d) + ['cmps']
    for i in c_df_call_d.itertuples():
        r = list(i[1:])
        r.append(get_cmp(i))

        eds.append(r)
    calls_touch = pandas.DataFrame(eds, columns = cols)
    calls_touch['date'] = calls_touch['timestamp'].apply(lambda x: datetime.datetime.fromtimestamp(x).date())
    
    return calls_touch

# Получаем все звонки 
sh = gc.open_by_key("1bMMb7RO-xHPxPe7fAwoypHtt__APaLF7E5UMHyFEXHU")
wk = sh.worksheet('nb_calls_bq')
list_of_dicts = wk.get_all_records()
calls_g_c = pandas.DataFrame(list_of_dicts)
calls_g_c = calls_g_c[calls_g_c['date_time'] !=  '']
calls_g_c = calls_g_c[calls_g_c['date_time'] !=  '-']

calls_g_c['caller'] = calls_g_c['caller'].astype(str)
calls_g_c['sale_type'] =calls_g_c['sale_type'].apply(lambda x: 0 if '-' == x else x)
calls_g_c['sale_type'] = calls_g_c['sale_type'].apply(lambda x: 0 if x == '' else x)
calls_g_c['date_time'] = calls_g_c['date_time'].apply(lambda x: x.replace('. ','.') )
calls_g_c['date_time'] = calls_g_c['date_time'].apply(lambda x: x.replace(' ',' 0') if len(x) == 18 else x )
calls_g_c['date_time'] = calls_g_c['date_time'].apply(lambda x: x.replace('.',':') )
calls_g_c['date_time'] = calls_g_c['date_time'].apply(lambda x: x.replace('2021','2021 ').replace('  ',' ') )
calls_g_c['date_time'] = calls_g_c['date_time'].apply(lambda x: datetime.datetime.strptime(x[:19],"%d:%m:%Y %H:%M:%S" ))
for i in calls_g_c.columns:
    calls_g_c[i] = calls_g_c[i].astype(str)
calls_g_c['sale_type'] = calls_g_c['sale_type'].astype(int)

# Обновлем всю таблу звонков
calls_g_c.to_gbq(f'sheets.nb_calls_site', project_id='m2-main', if_exists='replace', credentials=gbq_credential)

# Получаем последнюю дату CT
q  = '''
SELECT MAX(date) as l_dt FROM `sheets.calltouch_calls`
'''
last_date_ct = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
last_date = datetime.datetime.strptime(last_date_ct['l_dt'][0],"%Y-%m-%d").date()
end_date = datetime.datetime.today().date()-datetime.timedelta(days=1)

# Обновляем если есть новые данные
if last_date != end_date:
    print('NEWS!')
    call_touch_calls = get_ct_records(last_date,end_date,keys)
    call_touch_calls.to_gbq(f'sheets.calltouch_calls', project_id='m2-main', if_exists='append', credentials=gbq_credential)
    
    
# Создаём таблицу Потерянный звонков для Мобилок и др. целей
q = '''
SELECT * FROM `m2-main.sheets.nb_calls_site`
where caller not in (
    SELECT distinct(callerNumber)  FROM `m2-main.sheets.calltouch_calls`
)
'''
unidentified_calls = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
unidentified_calls.to_gbq(f'sheets.lost_calls', project_id='m2-main', if_exists='replace', credentials=gbq_credential)


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
    n  = ['callId','timestamp','callerNumber','callbackCall',
      'uniqueCall', 'targetCall', 'uniqTargetCall','successful',
      'source', 'medium','keyword',
      'utmSource','utmMedium', 'utmCampaign','utmContent','utmTerm',
      'clientId',
      'timestamp'
     ]
    df_call = pandas.DataFrame(params)
    
    drps = [ i for i in list(df_call.columns) if i not in n ]

    c_df_call = df_call.drop(columns= drps)
    c_df_call['date_time_msk'] = c_df_call['timestamp'].apply(lambda x: (datetime.datetime.utcfromtimestamp(x)+datetime.timedelta(hours=3)))
    return c_df_call


q  = '''
SELECT MAX(date_time_msk) as l_dt FROM `sheets.calltouch_calls_new`
'''
last_date_ct = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
start_date = last_date_ct['l_dt'][0].date()-datetime.timedelta(days=2)
end_date = datetime.datetime.today().date()-datetime.timedelta(days=1)

new_rows = get_calls(start_date,end_date)

# Обновляем если есть новые данные

if len(new_rows) >0 :
    client = bigquery.Client()
    
    query_job = client.query(
        f"""
        DELETE

        FROM `sheets.calltouch_calls_new`
        WHERE date_time_msk >= '{start_date}'
        """
    )
    results = query_job.result()
    new_rows.to_gbq(f'sheets.calltouch_calls_new', project_id='m2-main', if_exists='append', credentials=gbq_credential)
