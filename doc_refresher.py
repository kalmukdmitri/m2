import datetime
import pandas
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
import gspread

key_path = '/home/web_analytics/m2-main-cd9ed0b4e222.json'
gbq_credential = service_account.Credentials.from_service_account_file(key_path,)
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
             'https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(key_path, SCOPES)

gc = gspread.authorize(credentials)
def get_tabel_OPG(month):
    sheet_name_MSK = f'Заявки Мск {month}'
    sh = gc.open_by_key("1y_EEnKZNU48Pg2Lq7DI1cIBvotvlGp72zdDrEU8mVwU")
    wk = sh.worksheet(sheet_name_MSK)
    list_of_dicts = wk.get_all_records()
    calls_g_c = pandas.DataFrame(list_of_dicts)

    calls_g_c2 = calls_g_c[calls_g_c['Дата']!='']
    calls_g_c3 = calls_g_c2[calls_g_c['Статус обращения']=='QUIZGO']
    needed_M = ['Дата',
     'Номер телефона',
     'Имя',
     'Статус обращения',
     'кол-во комнат',
     'метраж',
     'Результат',
     'Комментарии']
    drps = [i for i in list(calls_g_c3.columns) if i not in needed_M] 
    calls_g_c3_MSK = calls_g_c3.drop(columns = drps)
    
    sheet_name_SPB = f'Заявки СПб {month}'
    wk = sh.worksheet(sheet_name_SPB)
    list_of_dicts = wk.get_all_records()
    calls_g_c = pandas.DataFrame(list_of_dicts)

    calls_g_c2 = calls_g_c[calls_g_c['Дата']!='']
    calls_g_c3 = calls_g_c2[calls_g_c['Статус обращения']=='QUIZGO']
    needed_s = ['Дата',
     'Номер телефона',
     'Имя',
     'Статус обращения',
     'кол-во комнат',
     'метраж',
     'Результат',
     'Комментарии (локация, степень готовности, военная ипотека, сертификаты)']
    drps = [i for i in list(calls_g_c3.columns) if i not in needed_s] 
    calls_g_c3_SBP = calls_g_c3.drop(columns = drps)
    calls_g_c3_SBP.columns = needed_M
    calls_g_c3_SBP['Регион'] = 'СПБ'
    calls_g_c3_MSK['Регион'] = 'МСК'
    full_res = pandas.concat([calls_g_c3_SBP, calls_g_c3_MSK]).reset_index(drop = True)
    return full_res

res_list = []
month_list = ["Октябрь", "Ноябрь"]
for i in month_list:
    res_list.append(get_tabel_OPG(i))
final = pandas.concat(res_list)

final_s = final[final['Регион'] == 'СПБ'].reset_index(drop = True)
final_m = final[final['Регион'] == 'МСК'].reset_index(drop = True)
def sheet_ready(df_r):
    for i in df_r:
        df_r[i]= df_r[i].astype(str)
    rows  = [list(df_r.columns)]
    for i in df_r.itertuples():
        ls=list(i)[1:]
        rows.append(ls)
    return rows

sh = gc.open_by_key("17p9apeWzq_Cg8HnyD25XT-UiHT5-xSI-94bzDjMVooY")
wk = sh.worksheet('МСК')
g_clop=sheet_ready(final_m)
wk.update('A1',g_clop)
wk = sh.worksheet('СПБ')
g_clop=sheet_ready(final_s)
wk.update('A1',g_clop)
