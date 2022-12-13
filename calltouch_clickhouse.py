import requests
import numpy
from numpy import dtype
import pandas as pd
from datetime import datetime
import os
from clickhouse_driver import Client
import json
import csv
import requests
import datetime
import pandas
import string
from collections import OrderedDict
from clickhouse_py  import clickhouse_pandas


key_path = '/home/kalmukds/m2-main-cd9ed0b4e222.json'
key_path_extra = '/home/kalmukds/other_keys.json'
# key_path = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\m2-main-cd9ed0b4e222.json'
# key_path_extra = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\other_keys.json'

f = open(key_path_extra, "r")
key_other = f.read()
keys = json.loads(key_other)['keys']['calltouch']

tokem = keys['tokem']
mod_id = keys['id']
cabinet = keys['cabinet']

def upload_multipart(table_name, df):
    
    dates_list = list(set(df['date']))
    dates_list.sort()
    
    for i in range(0,len(dates_list), 10):
        
        date_block = dates_list[i:i+10]
        regs_date = df[df['date'].apply(lambda x: x in date_block)]
        regs_date = regs_date.reset_index(drop=True)
        clk.insert(regs_date, table_name)

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
    c_df_call['date_time_msk'] = c_df_call['date_time_msk'].apply(lambda x : x.tz_localize(None))
    return c_df_call

clk  = clickhouse_pandas('kalmukds')
# clk.creat_table_df(part2, 'CALLTOUCH_JOURNAL')
q  = '''
SELECT MAX(date) as l_dt FROM external.CALLTOUCH_JOURNAL
'''
last_date_ct = clk.get_query_results(q)['l_dt'][0]
start_date = last_date_ct - datetime.timedelta(days=2)
end_date   = datetime.datetime.today().date() - datetime.timedelta(days=1)

new_rows = get_calls(start_date,end_date)
new_rows['date'] = new_rows['date_time_msk'].apply(lambda x: x.date())
for i in new_rows:
    if type(new_rows[i][0]) == numpy.int64:
        new_rows[i] = new_rows[i].astype(int)
    if type(new_rows[i][0]) == numpy.int32:
        new_rows[i] = new_rows[i].astype(int)  
if len(new_rows) > 0 :
    
    print(len(new_rows))
    res  = clk.get_query_results(
        f"""
        ALTER TABLE external.CALLTOUCH_JOURNAL DELETE WHERE date >= '{start_date}'
        """)
    
    upload_multipart('external.CALLTOUCH_JOURNAL', new_rows)