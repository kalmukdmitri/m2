import pandas_gbq
import requests
import datetime
import json
import pandas
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
import gspread
import string
from google.cloud import bigquery

key_path = '/home/web_analytics/m2-main-cd9ed0b4e222.json'

gbq_credential = service_account.Credentials.from_service_account_file(key_path,)
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
             'https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(key_path, SCOPES)
bigquery_client = bigquery.Client.from_service_account_json(key_path)

q = """
    delete from m2-main.sheets.NB_ALL_CALLS
    where extract(month from date_time) = extract(month from CURRENT_DATE()) 
  and extract(year from date_time) = extract(year from CURRENT_DATE())
"""
job = bigquery_client.query(q)

gc = gspread.authorize(credentials)
sh = gc.open_by_key("1DaZoAZjE_yg2pKyAxY_YqBT6hWXuS-elHWPFvzgANbQ")
wk = sh.worksheet('Лист1')
list_of_dicts = wk.get_all_records()

def de_BOM(s):

    s = s.encode('utf-8-sig')
    s = s.decode('utf-8')
    return s



calls_g_c = pandas.DataFrame(list_of_dicts)
calls_g_c = calls_g_c[calls_g_c['date_time'] !=  '']
calls_g_c = calls_g_c[calls_g_c['date_time'] !=  '-']
calls_g_c = calls_g_c.drop(columns = ['empt1', 'empt2', 'empt3','empt4'])
calls_g_c = calls_g_c[calls_g_c['date_broken'] != 'TRUE'].reset_index(drop=True)
# calls_g_c['comment'] = calls_g_c['comment'].apply(lambda x: x.replace('\\', '') if type(x) == str and '\\'  in x  else x )
# calls_g_c['comment'] = calls_g_c['comment'].apply(lambda x: x.replace('/', '') if type(x) == str and '/'  in x  else x )

# calls_g_c['comment'] = calls_g_c['comment'].apply(de_BOM)

calls_g_c['comment' = '-'
calls_g_c['date_time'] = calls_g_c['date_time'].apply(lambda x: x.replace('   ',' '))
calls_g_c['partner_source'] = calls_g_c['partner_source'].apply(lambda x: x if x not in ('','#N/A','#REF!') else '-')
calls_g_c['sold_sum'] = calls_g_c['sold_sum'].apply(lambda x: 0 if '-' == x else x)
calls_g_c['sold_sum'] = calls_g_c['sold_sum'].apply(lambda x: 0 if x == '' else x)
for i in calls_g_c.columns:
    calls_g_c[i] = calls_g_c[i].astype(str)
calls_g_c['sold_sum'] = calls_g_c['sold_sum'].astype(int)
calls_g_c['date_time'] = calls_g_c['date_time'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d %H:%M:%S" ))
calls_g_c = calls_g_c.drop(columns = ['date_broken'])
calls_g_c = calls_g_c[calls_g_c.date_time.dt.year == datetime.datetime.today().year] \
                    [calls_g_c.date_time.dt.month == datetime.datetime.today().month]
calls_g_c.to_gbq(f'sheets.NB_ALL_CALLS', project_id='m2-main', if_exists='append', credentials=gbq_credential)

sh = gc.open_by_key("1bTDaGyRRZzWMKS95gDKMwgHf7NScFwPbhbs1y-WCsWI")
wk = sh.worksheet('export_list')
list_of_dicts = wk.get_all_values()

df_raw = pandas.DataFrame(list_of_dicts[1:], columns = list_of_dicts[0])
df_raw = df_raw[df_raw['Date'].apply(lambda x: len(x) == 10)]
for i in list(df_raw.columns)[1:]:
    df_raw[i] = df_raw[i].apply(lambda x : float(x.replace(',','.')))
df_raw['Date'] = df_raw['Date'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
df_clean = df_raw.reset_index(drop = True)

ends = []
for i in df_clean.itertuples():
    row = [
        i.Date
    ]
    quize_saldo = i.quiez_gain - i.quiez_cost
    quize_gain_pure = 0 if quize_saldo < 0 else quize_saldo/2
    quize_cost_pure = i.quiez_cost if quize_saldo < 0 else i.quiez_cost+quize_saldo/2
    
    lk_saldo = i.lk_gain - i.lk_cost
    lk_gain_pure = 0 if lk_saldo < 0 else lk_saldo/2
    lk_cost_pure = i.lk_cost if lk_saldo < 0 else i.lk_cost+lk_saldo/2
    
    choise_saldo = i.choise_gain - i.choise_cost
    choise_gain_pure = 0 if choise_saldo < 0 else choise_saldo/2
    choise_cost_pure = i.choise_cost if choise_saldo < 0 else i.choise_cost+choise_saldo/2
    
    bot_saldo = i.bot_gain - i.bot_cost
    bot_gain_pure = 0 if bot_saldo < 0 else bot_saldo/2
    bot_cost_pure = i.bot_cost if quize_saldo < 0 else i.bot_cost+bot_saldo/2
    
    general_cost = i.quiez_cost + i.lk_cost + i.choise_cost + i.bot_cost
    general_gain = i.quiez_gain + i.lk_gain + i.choise_gain + i.bot_gain
    general_gain_pure = quize_gain_pure + lk_gain_pure + choise_gain_pure + bot_gain_pure
    general_cost_pure = quize_cost_pure + lk_cost_pure + choise_cost_pure + bot_cost_pure
    
    row_extentdy = [
        i.quiez_cost,
        i.quiez_gain,
        quize_gain_pure,
        quize_cost_pure,
        i.lk_cost,
        i.lk_gain,
        lk_gain_pure,
        lk_cost_pure,
        i.choise_cost,
        i.choise_gain,
        choise_gain_pure,
        choise_cost_pure,
        i.bot_cost,
        i.bot_gain,
        bot_gain_pure,
        bot_cost_pure,
        general_cost,
        general_gain,
        general_gain_pure,
        general_cost_pure
    ]
    
    row.extend(row_extentdy)
    
    ends.append(row)
    
cols = ['Date', 'quiez_cost', 'quiez_gain', 'quize_gain_pure',
       'quize_cost_pure', 'lk_cost', 'lk_gain', 'lk_gain_pure', 'lk_cost_pure',
       'choise_cost', 'choise_gain', 'choise_gain_pure', 'choise_cost_pure',
       'bot_cost', 'bot_gain', 'bot_gain_pure', 'bot_cost_pure',
       'general_cost', 'general_gain', 'general_gain_pure',
       'general_cost_pure']
mambery_calc = pandas.DataFrame(ends, columns = cols)
mambery_calc['Partner_source'] = 'Mumbery'
mambery_calc.to_gbq(f'sheets.mumbery_data', project_id='m2-main', if_exists='replace', credentials=gbq_credential)

# wk = sh.worksheet('export_list_spb')
# list_of_dicts = wk.get_all_values()
# full_spb_data = pandas.DataFrame(list_of_dicts[1:], columns = list_of_dicts[0])
# full_spb_data.columns = ['Date','costs_raw', 'gains']
# full_spb_data = full_spb_data[full_spb_data['Date'].apply(lambda x: type(x) == str and len(x) == 10 )]
# full_spb_data['costs_raw'] = full_spb_data['costs_raw'].apply(lambda x: x.replace('р.','').replace('\xa0','').split(',')[0]).astype(int)
# full_spb_data['gains'] = full_spb_data['gains'].apply(lambda x: x.replace('р.','').replace('\xa0','').split(',')[0]).astype(int)
# full_spb_data = full_spb_data[full_spb_data['costs_raw'].apply(lambda x: x !=0 )]
# ends = []
# for i in full_spb_data.itertuples():
#     row = [
#         i.Date
#     ]
#     quize_saldo = i.gains - i.costs_raw
#     quize_gain_pure = 0 if quize_saldo < 0 else quize_saldo/2
#     quize_cost_pure = i.costs_raw if quize_saldo < 0 else i.costs_raw + (quize_saldo/2)
    
#     row_extentdy = [
#         i.costs_raw,
#         i.gains,
#         quize_gain_pure,
#         quize_cost_pure,
#     ]
    
#     row.extend(row_extentdy)
    
#     ends.append(row)
    
# cols = ['Date', 'spb_cost', 'spb_gain', 'spb_gain_pure',
#        'spb_cost_pure']
# spb_mambery_calc = pandas.DataFrame(ends, columns = cols)



# spb_mambery_calc.to_gbq(f'sheets.spb_mumbery_data', project_id='m2-main', if_exists='replace', credentials=gbq_credential)


sh = gc.open_by_key("17c4a0gmXkbAEaDgoo03YkdRKCiB7nTsM_077GJC3noo")
wk = sh.worksheet('export_list')
list_of_dicts = wk.get_all_values()
dt = pandas.DataFrame(list_of_dicts)
dt.columns = ['week1', 'market_cost', 'gain_all', 'gain_vas' , 'VAS_manual']
dt = dt[1:]
dt = dt[dt["week1"].apply(lambda x: x != '' and '*' not in  x )].reset_index(drop = True)
for i in dt:
    dt[i] = dt[i].apply(lambda x: x.replace('\xa0','').replace('р.','').replace(',','.'))
    try:
        dt[i] = dt[i].astype(float)
        dt[i] = dt[i].astype(int)
    except:
        continue
dt['week1'] = dt['week1'].apply(lambda x: datetime.datetime.strptime(x,"%d.%m.%Y"))
dt['gain_vas'] = dt['gain_vas']+ dt['VAS_manual']
dt= dt.drop(columns = "VAS_manual")
ends = []
for week in dt.itertuples():
    for stp in range(7):
        week_num = week.week1.isocalendar()[1]
        date = week.week1 + datetime.timedelta(days=stp)
        dates = [
        week.week1,
        week_num,
        date,
        week.market_cost/7,
        week.gain_all/7,
        week.gain_vas/7  
        ]
        ends.append(dates)
cols = ['week1',
        'isoweek',
        'date',
        'market_cost',
        'gain_all',
        'gain_vas'
       ]
calends = pandas.DataFrame(ends , columns =cols )
calends = calends.drop(columns = ['isoweek','week1'])
calends.to_gbq(f'GOOGLE_SHEETS_DATA.UP_REPORTED_DATA_2022', project_id='m2-main', if_exists='replace', credentials=gbq_credential)