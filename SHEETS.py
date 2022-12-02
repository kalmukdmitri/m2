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

def b64_hid(s):

    b = s.encode("UTF-8")
    e = base64.b64encode(b)
    s1 = e.decode("UTF-8")
    return s1

def sums_cleans(MONEYS):
    if type(MONEYS) == str:
        MONEYS = 0
    return MONEYS

def ib_df_cleanup(IB_CALLS):
    IB_CALLS = IB_CALLS[IB_CALLS['Parsable'] == 'TRUE']
    IB_CALLS = IB_CALLS.drop(columns = ['Parsable'])
    res = []
    for i in IB_CALLS.itertuples():
        row = list(i[1:])
        date_start = i.Date1
        if date_start == '':
            date_start = i.Date2
        date_end = i.Date2
        if date_end == '':
            date_end = i.Date1
        row.extend([date_start,date_end])
        res.append(row)

    cols = ['Client', 'Manager', 'Source', 'Date1', 'Date2', 'City',
           'Partner', 'Type', 'Bank', 'Sum_mort', 'Gain', 'date_created' , 'date_ended']
    ib_result = pandas.DataFrame(res, columns = cols)
    ib_result['date_created'] = ib_result['date_created'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d" ))
    ib_result['date_ended'] = ib_result['date_ended'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d" ))
    ib_result = ib_result.drop(columns = ['Date1' , 'Date2'])

    ib_result['Sum_mort'] = ib_result['Sum_mort'].apply(sums_cleans)
    ib_result['Gain'] = ib_result['Gain'].apply(sums_cleans)
    ib_result['Client'] = ib_result['Client'].apply(b64_hid)
    ib_result['date'] = ib_result['date_created']
    ib_result = ib_result.reset_index(drop=True)
    return ib_result


def max_sintise(str_raw):
    if type(str_raw) == str:
        legal = """АаБбВвГгДдЕеЁёЖжЗзИиЙйКкЛлМмНнРрОоПпСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЭэЮюЯя0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXY/Z!"#$%&\'()*+,-:;., !:«»"""
        str_raw = "".join([i for i in str_raw if i in legal])
    else:
        str_raw = str_raw
    return str_raw


def NB_CALLS_CLEANUP(calls_g_c):

    calls_g_c = calls_g_c[calls_g_c['date_time'] !=  '']
    calls_g_c = calls_g_c[calls_g_c['date_time'] !=  '-']
    calls_g_c = calls_g_c.drop(columns = ['empt1', 'empt2', 'empt3','empt4'])
    calls_g_c = calls_g_c[calls_g_c['date_broken'] != 'TRUE'].reset_index(drop=True)
    calls_g_c['comment'] = '-'
    calls_g_c['date_time'] = calls_g_c['date_time'].apply(lambda x: x.replace('   ',' '))
    calls_g_c['partner_source'] = calls_g_c['partner_source'].apply(lambda x: x if x not in ('','#N/A','#REF!') else '-')
    calls_g_c['sold_sum'] = calls_g_c['sold_sum'].apply(lambda x: 0 if '-' == x else x)
    calls_g_c['sold_sum'] = calls_g_c['sold_sum'].apply(lambda x: 0 if x == '' else x)
    for i in calls_g_c.columns:
        calls_g_c[i] = calls_g_c[i].astype(str)
        calls_g_c[i] = calls_g_c[i].apply(max_sintise)
    calls_g_c['sold_sum'] = calls_g_c['sold_sum'].astype(int)
    calls_g_c['date_time'] = calls_g_c['date_time'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d %H:%M:%S" ))
    calls_g_c = calls_g_c.drop(columns = ['date_broken'])
    calls_g_c = calls_g_c[calls_g_c.date_time.dt.year == datetime.datetime.today().year] \
                        [calls_g_c.date_time.dt.month == datetime.datetime.today().month]
    calls_g_c['date'] = calls_g_c['date_time'].apply(lambda x: x.date())
    calls_g_c = calls_g_c.reset_index(drop=True)
    return calls_g_c

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


#  PG LOAD

# key_path_extra = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\pg_keys.json'
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
regs = regs[['user_code','user_email','user_phone','role','date']]
regs = regs.sort_values(['date']).reset_index(drop=True)

clk  = clickhouse_pandas('web') 
res  = clk.get_query_results(
    f"""
    ALTER TABLE pg.PG_REGS DELETE  WHERE 1 = 1
    """)

regs = regs.sort_values(['date']).reset_index(drop=True)

upload_multipart('pg.PG_REGS', regs)

q = '''
SELECT * FROM "MART_AUTH"."REGISTRATIONS"  '''

regs = get_df(q, engine)
regs['date'] = regs['registration_date']
regs = regs[['user_code',
 'user_email',
 'user_phone',
 'report_type',
 'role',
 'role_detail',
 'date']]
regs = regs.sort_values(['date']).reset_index(drop=True)

clk  = clickhouse_pandas('web') 
res  = clk.get_query_results(f"""
ALTER TABLE pg.PG_REGS_FULL DELETE WHERE 1 = 1""")

regs = regs.sort_values(['date']).reset_index(drop=True)

upload_multipart('pg.PG_REGS_FULL', regs)

regs = regs.drop(columns = ['user_email','user_phone'])

regs = regs.reset_index(drop=True)

regs['date'] = regs['date'].astype(str)

regs.to_gbq(f'EXTERNAL_DATA_SOURCES.PG_DAYLY_RELOADED', project_id='m2-main', chunksize=20000, if_exists='replace', credentials=gbq_credential)

# DOWNLOAD
key_path = '/home/kalmukds/m2-main-cd9ed0b4e222.json'
# key_path = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\m2-main-cd9ed0b4e222.json'

gbq_credential = service_account.Credentials.from_service_account_file(key_path,)
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
             'https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(key_path, SCOPES)

gc = gspread.authorize(credentials)

sh = gc.open_by_key("1xMDWCSt6Br5kCp1ekLGlIFLAGrHvWCaljgnXBngUieI")
wk = sh.worksheet('list')
list_of_dicts = wk.get_all_records()
IB_CALLS = pandas.DataFrame(list_of_dicts)

sh = gc.open_by_key("1ed1eWBT8B5_pTDYcihY64Q6Gt4hLXeRAiKSJn1KEU9o")
wk = sh.worksheet('Лист1')
list_of_dicts = wk.get_all_records()
calls_g_c = pandas.DataFrame(list_of_dicts)

# CLEAN
IB_CALLS_CLEAN = ib_df_cleanup(IB_CALLS)
NB_CALLS_CLEAN = NB_CALLS_CLEANUP(calls_g_c)

min_record = str(min(NB_CALLS_CLEAN['date']))

res  = clk.get_query_results(
    f"""
    ALTER TABLE google_sheets.IB DELETE WHERE 1=1
    """)

res  = clk.get_query_results(
    f"""
    ALTER TABLE google_sheets.NB DELETE WHERE date >= '{min_record}'
    """)

# LOAD
upload_multipart('google_sheets.IB' , IB_CALLS_CLEAN)
upload_multipart('google_sheets.NB' , NB_CALLS_CLEAN)