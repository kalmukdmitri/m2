from google.cloud import bigquery
from google.oauth2 import service_account
from clickhouse_py  import clickhouse_pandas, clickhouse_logger
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


def get_df(query, engine ):
    df_check = engine.execute(query)
    df_q = pandas.DataFrame(df_check.fetchall(), columns = df_check.keys())
    engine.dispose()
    return df_q


# key_path = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\m2-main-cd9ed0b4e222.json'
key_path = '/home/kalmukds/m2-main-cd9ed0b4e222.json'
gbq_credential = service_account.Credentials.from_service_account_file(key_path,)

# key_path_extra = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\pg_keys.json'
key_path_extra = '/home/kalmukds/pg_keys.json'
f = open(key_path_extra, "r")
key_other = f.read()
keys = json.loads(key_other)['pg']
engine = create_engine(keys)


# q = '''WITH PAGE AS (
# select 
# date(dateHourMinute) as view_dt,
# dimension4,
# MAX(case when pagepath like '%personal-area/services/deal/create%' then 1 else 0 end) as creat,
# MAX(case when pagepath like  '%personal-area/services/deal%' and pagepath like '%members%' then 1 else 0 end) as next_step

# from `UA_REPORTS.PAGE_VIEWS`
# where pagepath like  '%personal-area/services/deal%'
# and date(dateHourMinute) > '2022-09-30'

# group by 1,2),
# sess AS (
# SELECT
# session_id,
# user_id,
# dimension2
# FROM `TEST_MART.SESSIONS_TABLE ` 
# LEFT JOIN (SELECT dimension1,dimension2 FROM `UA_REPORTS.USERS_DT` GROUP BY 1,2) on dimension1 = user_id
# -- WHERE dimension2 is not null
# group by 1,2,3
# )
# SELECT  dimension2 as user_code, max(view_dt) as view_dt FROM PAGE
# inner JOIN sess on session_id=dimension4
# where next_step = 0
# and creat = 1
# -- and dimension2 is not null
# group by dimension2

# '''
clk  = clickhouse_pandas('ga')
q = '''
WITH PAGE AS (
select 
toDate(dateHourMinute) as view_dt,
dimension4,
max(case when pagepath like '%personal-area/services/deal/create%' then 1 else 0 end) as creat,
max(case when pagepath like  '%personal-area/services/deal%' and pagepath like '%members%' then 1 else 0 end) as next_step
from ga.PAGE_VIEWS
where pagepath like  '%personal-area/services/deal%'
and toDate(dateHourMinute) > '2022-09-30'
group by view_dt,dimension4),
sess AS (
SELECT
session_id,
user_id,
client_id
FROM mart.SESSIONS_TABLE
LEFT JOIN (SELECT dimension1,dimension2 FROM ga.USERS_DT GROUP BY dimension1,dimension2) as u on dimension1 = client_id

group by session_id,
user_id,
client_id
)
SELECT  user_id as user_code,max(view_dt)  as view_dt FROM PAGE
inner JOIN sess on session_id=dimension4
where next_step = 0
and creat = 1
group by user_id'''




pg = clk.get_query_results(q)


q = """select
a.user_code
,role
,MIN(c.registration_date) as user_registration_date
,MAX(a.user_name) as user_name
,MAX(person_name) as person_name
,MAX(person_surname) as person_surname
,MAX(user_phone) as user_phone
,MAX(user_email) as user_email
from "MART_AUTH"."ACTUAL_USER" a
left join  "MART_AUTH"."REGISTRATIONS" c
on a.user_code=c.user_code
where user_phone is not null
group by 1,2
"""
regs = get_df(q, engine =engine) 
load = pg.merge(regs, left_on='user_code', right_on='user_code')
load = load.sort_values('view_dt').reset_index(drop=True)
gbq_credential = service_account.Credentials.from_service_account_file(key_path,)
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
             'https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(key_path, SCOPES)

gc = gspread.authorize(credentials)
def sheet_ready(df_r):
    for i in df_r:
        df_r[i]= df_r[i].astype(str)
    rows  = [list(df_r.columns)]
    for i in df_r.itertuples():
        ls=list(i)[1:]
        rows.append(ls)
    return rows

drops = ['user_registration_date',
'user_name',
'person_surname']
load = load.drop(columns = drops)

sh = gc.open_by_key("1hU11cmwMtNC8PfVg9gci7Gg6oaQ5dsHwTCcduLE6hJ4")
wk = sh.worksheet('Лист1')
g_clop=sheet_ready(load)
wk.update('A1',g_clop)