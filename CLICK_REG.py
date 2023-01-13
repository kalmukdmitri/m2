from clickhouse_py  import clickhouse_pandas, clickhouse_logger
from google.cloud import bigquery
from google.oauth2 import service_account
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine
import base64
import datetime
import gspread
import json
import pandas
import pandas_gbq
import requests
import string


def get_df(query, engine):
    df_check = engine.execute(query)
    df_q = pandas.DataFrame(df_check.fetchall(), columns = df_check.keys())
    engine.dispose()
    return df_q

def upload_multipart(table_name, df):
    
    dates_list = list(set(df['date']))
    dates_list.sort()
    
    for i in range(0,len(dates_list), 10):
        
        date_block = dates_list[i:i+10]
        regs_date = df[df['date'].apply(lambda x: x in date_block)]
        regs_date = regs_date.reset_index(drop=True)
        clk.insert(regs_date, table_name)


key_path_extra = '/home/kalmukds/pg_keys.json'

f = open(key_path_extra, "r")
key_other = f.read()
keys = json.loads(key_other)['pg']
engine = create_engine(keys)


q = '''
SELECT * FROM "MART_AUTH"."REGISTRATIONS" 
'''
regs_full = get_df(q, engine)
regs_limited = regs_full[regs_full['report_type'] == 'Зарегистрированный пользователь (установлен пароль)'].reset_index(drop=True)

regs_limited['date'] = regs_limited['registration_date'] 
regs_limited = regs_limited[['user_code','user_email','user_phone','role','date','registration_msk_ts']]
regs_limited = regs_limited.sort_values(['date']).reset_index(drop=True)

clk  = clickhouse_pandas('web') 
res  = clk.get_query_results(
f"""
ALTER TABLE pg.PG_REGS DELETE WHERE 1 = 1""")

regs_limited = regs_limited.sort_values(['date']).reset_index(drop=True)

upload_multipart('pg.PG_REGS', regs_limited)

regs_full['date'] = regs_full['registration_date']

regs_full = regs_full[[
 'user_code',
 'user_email',
 'registration_msk_ts',
 'user_phone',
 'report_type',
 'role',
 'role_detail',
 'date']]

regs_full = regs_full.sort_values(['date']).reset_index(drop=True)

clk  = clickhouse_pandas('web') 
res  = clk.get_query_results(f"""
ALTER TABLE pg.PG_REGS_FULL DELETE WHERE 1 = 1""")

regs_full = regs_full.sort_values(['date']).reset_index(drop=True)

upload_multipart('pg.PG_REGS_FULL', regs_full)