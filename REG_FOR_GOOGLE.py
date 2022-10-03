import pandas_gbq
from clickhouse_py  import clickhouse_pandas, clickhouse_logger
from sqlalchemy import create_engine
from google.oauth2 import service_account
import datetime
import pandas
import json
import pandas_gbq
import requests

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

key_path = '/home/kalmukds/m2-main-cd9ed0b4e222.json'
# key_path = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\m2-main-cd9ed0b4e222.json'
gbq_credential = service_account.Credentials.from_service_account_file(key_path,)


q = '''
SELECT * FROM "MART_AUTH"."REGISTRATIONS" 
where report_type = 'Зарегистрированный пользователь (установлен пароль)' '''
regs = get_df(q, engine)
regs['date'] = regs['registration_date'] 
regs = regs.drop(columns = ['registration_date','registration_week','registration_month','tech_load_ts','cnt_registrations','report_type' ])
regs = regs.sort_values(['date']).reset_index(drop=True)
regs = regs.drop(columns = ['user_email','user_phone'])

regs = regs.reset_index(drop=True)
regs['date'] = regs['date'].astype(str)

regs.to_gbq(f'EXTERNAL_DATA_SOURCES.PG_DAYLY_RELOADED', project_id='m2-main', chunksize=20000, if_exists='replace', credentials=gbq_credential)

q = '''
Select
user_code,
role_main,
report_date
from "MART_AUTH"."AUTHORIZATIONS"
where report_type = 'По дням'
group by user_code, role_main, report_date
'''
authes = get_df(q, engine)
authes.to_gbq(f'EXTERNAL_DATA_SOURCES.AUTH_PG', project_id='m2-main', chunksize=20000, if_exists='replace', credentials=gbq_credential)