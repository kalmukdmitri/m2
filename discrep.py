from clickhouse_py  import clickhouse_pandas, clickhouse_logger
from datetime import datetime
from ga_connector_click import ga_connect
from google.oauth2 import service_account
from googleapiclient.discovery import build
from clickhouse_driver import Client
from numpy import dtype
from oauth2client.service_account import ServiceAccountCredentials
import csv
import datetime
import json
import numpy
import os
import pandas
import requests
import string
import sys
import urllib
import pandas_gbq
from metrica import metrica
import gspread


#key_path = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\m2-main-cd9ed0b4e222.json'
key_path = '/home/kalmukds/m2-main-cd9ed0b4e222.json'
gbq_credential = service_account.Credentials.from_service_account_file(key_path,)



metr = metrica()
dimensions = ''
metrics = 'ym:s:visits,ym:s:users'
date1 = datetime.date(2022,10,27)
date2 = datetime.datetime.today().date()


metric_date = metr.pd_request(date1,date2,metrics,dimensions)
metric_date.columns = ['metrica_sessions','metrica_users','date']
metric_date['metrica_sessions'] = metric_date['metrica_sessions'].astype(int)
metric_date['metrica_users'] = metric_date['metrica_users'].astype(int)



q_matomo = """

SELECT 
    toDate(visit_first_action_time) as date,   
    count(distinct idvisit) as matomo_session_ids,
    count(distinct idvisitor) as matomo_users_ids
FROM newMatomo.matomo_log_visit
where visit_first_action_time > '2022-10-27'
group by date
order by date desc;

"""

q_ga = """
SELECT date,
count(distinct session_id) as ga_sessions_ga,
count(distinct user_id) as ga_users_ga 
FROM `m2-main.TEST_MART.SESSIONS_TABLE `
where date > '2022-10-26'
group by date
order by date

"""

table_df_bq = pandas_gbq.read_gbq(q_ga, project_id='m2-main', credentials=gbq_credential)
table_df_bq['date'] = table_df_bq['date'].astype(str)
table_df_bq = table_df_bq.fillna(0)

clk  = clickhouse_pandas('ga')
matomo = clk.get_query_results(q_matomo)
matomo['date'] = matomo['date'].astype(str)
matomo = matomo.fillna(0)

g_m = pandas.merge(matomo, table_df_bq, on="date", how ='inner')
m_g_m = pandas.merge(g_m, metric_date, on="date", how ='inner')
m_g_m = m_g_m.fillna(0)

def sheet_ready(df_r):
    for i in df_r:
        df_r[i]= df_r[i].astype(str)
    rows  = [list(df_r.columns)]
    for i in df_r.itertuples():
        ls=list(i)[1:]
        rows.append(ls)
    return rows

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
             'https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(key_path, SCOPES)

gc = gspread.authorize(credentials)


sh = gc.open_by_key("1qP3C-s1U3onu-bQEq2N_Lvb1pG_u622qs-55F-K4Sww")
wk = sh.worksheet('Лист1')
g_clop=sheet_ready(m_g_m)
wk.update('A1',g_clop)