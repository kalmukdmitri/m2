import pandas_gbq
import requests
import datetime
import json
import pandas
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
import gspread
import string
import bigquery_logger
from google.cloud import bigquery

key_path = '/home/web_analytics/m2-main-cd9ed0b4e222.json'

gbq_credential = service_account.Credentials.from_service_account_file(key_path,)
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
             'https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(key_path, SCOPES)
bigquery_client = bigquery.Client.from_service_account_json(key_path)

table_log = bigquery_logger.bq_logger("sheets.NB_ALL_CALLS")

try:
    
    start = datetime.datetime.today().date()
    end =  datetime.datetime.today().date() - datetime.timedelta(days=1)
    table_log.add_data_start(str(start))
    table_log.add_data_end(str(end))
    
    


    def max_sintise(str_raw):
        if type(str_raw) == str:
            legal = """АаБбВвГгДдЕеЁёЖжЗзИиЙйКкЛлМмНнРрОоПпСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЭэЮюЯя0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXY/Z!"#$%&\'()*+,-:;., !:«»"""
            str_raw = "".join([i for i in str_raw if i in legal])
        else:
            str_raw = str_raw
        return str_raw

    gc = gspread.authorize(credentials)
    sh = gc.open_by_key("1ed1eWBT8B5_pTDYcihY64Q6Gt4hLXeRAiKSJn1KEU9o")
    wk = sh.worksheet('Лист1')
    list_of_dicts = wk.get_all_records()
    calls_g_c = pandas.DataFrame(list_of_dicts)
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
    
    q = f"""
        delete from m2-main.sheets.NB_ALL_CALLS
        where  date_time >= '{min_record}' 
    """
    job = bigquery_client.query(q)
    
    calls_g_c.to_gbq(f'sheets.NB_ALL_CALLS', project_id='m2-main', if_exists='append', credentials=gbq_credential)
    
    print(len(calls_g_c))
    table_log.add_rows_recieved(len(calls_g_c))
    table_log.add_rows_updated(len(calls_g_c))
    table_log.no_errors_found()

except:
    table_log.errors_found(str(sys.exc_info()[1]))