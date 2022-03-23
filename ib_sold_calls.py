import pandas_gbq
import requests
import datetime
import json
import pandas
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
import gspread
import base64

key_path = '/home/web_analytics/m2-main-cd9ed0b4e222.json'
key_path_extra = '/home/web_analytics/other_keys.json'

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
def sums_cleans(MONEYS):
    if type(MONEYS) == str:
        MONEYS = 0
    return MONEYS

ib_result['Sum_mort'] = ib_result['Sum_mort'].apply(sums_cleans)
ib_result['Gain'] = ib_result['Gain'].apply(sums_cleans)

def b64_hid(s):

    b = s.encode("UTF-8")
    e = base64.b64encode(b)
    s1 = e.decode("UTF-8")
    return s1
ib_result['Client'] = ib_result['Client'].apply(b64_hid)

ib_result.to_gbq(f'GOOGLE_SHEETS_DATA.IB_FINISED', project_id='m2-main', if_exists='replace', credentials=gbq_credential)

def get_tabel_OPG(month):
    sheet_name_MSK = f'Заявки Мск {month}'
    sh = gc.open_by_key("1idE-rvW68Aby5bysuHO5T2vm813BdvoHnrHxGKr9fgk")
    wk = sh.worksheet(sheet_name_MSK)
    list_of_dicts = wk.get_all_records()
    calls_g_msk = pandas.DataFrame(list_of_dicts)

    
    calls_g_msk = calls_g_msk[calls_g_msk['Дата']!='']
    
    sheet_name_SPB = f'Заявки СПб {month}'
    wk = sh.worksheet(sheet_name_SPB)
    list_of_dicts = wk.get_all_records()
    calls_g_spb = pandas.DataFrame(list_of_dicts)

    calls_g_spb = calls_g_spb[calls_g_spb['Дата']!='']

    full_res = pandas.concat([calls_g_msk, calls_g_spb]).reset_index(drop = True)
    return full_res



res_list = []
month_list = ["Январь", "Февраль", "Март"]
for i in month_list:
    res_list.append(get_tabel_OPG(i))
final = pandas.concat(res_list).reset_index(drop = True)
needed_M = ['Источник', 'Дата', 'Номер телефона',
'Статус обращения', 'Рекламный источник',
 'Срок покупки','Результат','Стоимость', 'Способ покупки']
drps = [i for i in list(final.columns) if i not in needed_M] 
final_opg = final.drop(columns = drps)
final_opg['Дата'] = final_opg['Дата'].apply(lambda x: x.replace('2022-01-28' , '28-01-2022'))
final_opg['Дата'] = final_opg['Дата'].apply(lambda x: datetime.datetime.strptime(x.replace('.','-'),"%d-%m-%Y" ))
cols = ['SOURCE_OPG', 'DATE', 'CALLER_NUMBER',
'PARTNER_TYPE', 'AD_SOURCE',
 'TIME_BUY','RESULT_CALL','SALE_AMOUNT', 'METHOD_BUY']
print('update')

final_opg.columns = cols
final_opg['SALE_AMOUNT'] = final_opg['SALE_AMOUNT'].apply(lambda x : x if type(x) == int else 0)
final_opg['METHOD_BUY'] = final_opg['METHOD_BUY'].astype(str)
final_opg.to_gbq(f'GOOGLE_SHEETS_DATA.OPG_CALLS', project_id='m2-main', if_exists='replace', credentials=gbq_credential)