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
from transliterate import translit, get_available_language_codes

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

# key_path_extra = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\pg_keys.json'
key_path_extra = '/home/kalmukds/pg_keys.json'


f = open(key_path_extra, "r")
key_other = f.read()
keys = json.loads(key_other)['pg']
engine = create_engine(keys)

clk  = clickhouse_pandas('ga')
last_vas = 'select max(date) as l_dt from pg.V_CL_PHONE_CLICK_BY_PARTNERS_VAS_new;'
last_all = 'select max(date) as l_dt from pg.V_CL_PHONE_CLICK_BY_PARTNERS_new;'

q  = last_vas
last_vas_dt = clk.get_query_results(q)['l_dt'][0]

q  = last_all
last_all_dt = clk.get_query_results(q)['l_dt'][0]


q = f'''
SELECT * FROM "USER_KALMUKDS"."V_CL_PHONE_CLICK_BY_PARTNERS_new" 
where "Дата" > '{last_vas_dt}'
 '''
V_CL_PHONE_CLICK_BY_PARTNERS_new = get_df(q, engine)


q = f'''
SELECT * FROM "USER_KALMUKDS"."V_CL_PHONE_CLICK_BY_PARTNERS_VAS_new" 
where  "Дата" > '{last_all_dt}'
'''
V_CL_PHONE_CLICK_BY_PARTNERS_VAS_new = get_df(q, engine)

V_CL_PHONE_CLICK_BY_PARTNERS_new.columns = [
 'date',
 'Week',
 'Month',
 'Chanell',
 'Geo',
 'Partner',
 'ALL_Snippet_Views',
 'ALL_Card_Opens',
 'ALL_SERP_Clops',
 'ALL_Card_Clops',
 'ALL_Call_estimated',
 'ALL_Clops',
 'VAS_Snipped_views',
 'VAS_Card_Opens',
 'VAS_SERP_Clops',
 'VAS_Card_Clops',
 'VAS_Clops',
 'VAS_Call_estimated',
 'Conversion_to_call' ]
V_CL_PHONE_CLICK_BY_PARTNERS_new['Geo'] = V_CL_PHONE_CLICK_BY_PARTNERS_new['Geo'].astype(str)
V_CL_PHONE_CLICK_BY_PARTNERS_new['Partner'] = V_CL_PHONE_CLICK_BY_PARTNERS_new['Partner'].astype(str)
V_CL_PHONE_CLICK_BY_PARTNERS_new['Week'] =  V_CL_PHONE_CLICK_BY_PARTNERS_new['Week'].astype(int)
V_CL_PHONE_CLICK_BY_PARTNERS_new['Month'] = V_CL_PHONE_CLICK_BY_PARTNERS_new['Month'].astype(int)
V_CL_PHONE_CLICK_BY_PARTNERS_new['Partner'] = V_CL_PHONE_CLICK_BY_PARTNERS_new['Partner'].apply(lambda x :  '' if x == 'nan' else x)
V_CL_PHONE_CLICK_BY_PARTNERS_VAS_new.columns = [
 'date',
 'Week',
 'Month',
 'Chanell',
 'Geo',
 'Partner',
 'VAS_Snipped_views',
 'VAS_Card_Opens',
 'VAS_SERP_Clops',
 'VAS_Card_Clops',
 'VAS_Clops',
 'VAS_Call_estimated',
 'Conversion_to_call' ]
V_CL_PHONE_CLICK_BY_PARTNERS_VAS_new['VAS_Call_estimated'] = V_CL_PHONE_CLICK_BY_PARTNERS_VAS_new['VAS_Call_estimated'].astype('float64')
V_CL_PHONE_CLICK_BY_PARTNERS_VAS_new['Geo'] = V_CL_PHONE_CLICK_BY_PARTNERS_VAS_new['Geo'].astype(str)
V_CL_PHONE_CLICK_BY_PARTNERS_VAS_new['Partner'] = V_CL_PHONE_CLICK_BY_PARTNERS_VAS_new['Partner'].astype(str)
V_CL_PHONE_CLICK_BY_PARTNERS_VAS_new['Chanell'] = V_CL_PHONE_CLICK_BY_PARTNERS_VAS_new['Chanell'].astype(str)
V_CL_PHONE_CLICK_BY_PARTNERS_VAS_new['Week'] =  V_CL_PHONE_CLICK_BY_PARTNERS_VAS_new['Week'].astype(int)
V_CL_PHONE_CLICK_BY_PARTNERS_VAS_new['Month'] = V_CL_PHONE_CLICK_BY_PARTNERS_VAS_new['Month'].astype(int)
V_CL_PHONE_CLICK_BY_PARTNERS_VAS_new = V_CL_PHONE_CLICK_BY_PARTNERS_VAS_new.fillna(0)
V_CL_PHONE_CLICK_BY_PARTNERS_VAS_new['Partner'] = V_CL_PHONE_CLICK_BY_PARTNERS_VAS_new['Partner'].apply(lambda x :  '' if x == 'nan' else x)

def upload_multipart(table_name, df):
    
    dates_list = list(set(df['date']))
    dates_list.sort()
    
    for i in range(0,len(dates_list), 10):
        print(i)
        date_block = dates_list[i:i+10]
        regs_date = df[df['date'].apply(lambda x: x in date_block)]
        regs_date = regs_date.reset_index(drop=True)
        clk.insert(regs_date, table_name)

upload_multipart('pg.V_CL_PHONE_CLICK_BY_PARTNERS_new', V_CL_PHONE_CLICK_BY_PARTNERS_new)
upload_multipart('pg.V_CL_PHONE_CLICK_BY_PARTNERS_VAS_new', V_CL_PHONE_CLICK_BY_PARTNERS_VAS_new)
