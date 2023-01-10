from clickhouse_py  import clickhouse_pandas, clickhouse_logger
from imbox import Imbox
import pandas
import pandas_gbq
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import datetime
import gspread
import os

clk  = clickhouse_pandas('ga')

def sheet_ready(df_r):
    
    for i in df_r:
        df_r[i]= df_r[i].astype(str)
    rows  = [list(df_r.columns)]
    for i in df_r.itertuples():
        ls=list(i)[1:]
        rows.append(ls)
    return rows

def parse_json_answers(answers):
    ans = json.loads(answers.replace("\'", "\""))
    ans = {i['key']:i for i in ans}
    return ans

def parse_utm(utms):
    utms = utms.split('&')
    params = {
    'quiz_source': "",
    'utm_source':"",
    'utm_medium':"",
    'utm_campaign':"",
    'utm_content':"",
    'utm_term':""}
    for i in utms:
        if '=' in i:
            param,vals = i.split('=')[0], "=".join(i.split('=')[1:])
            params[param] = vals
    return params

def upload_multipart(table_name, df):
    
    dates_list = list(set(df['date']))
    dates_list.sort()
    
    for i in range(0,len(dates_list), 10):
        
        date_block = dates_list[i:i+10]
        regs_date = df[df['date'].apply(lambda x: x in date_block)]
        regs_date = regs_date.reset_index(drop=True)
        clk.insert(regs_date, table_name)

def parse_body(bod):
    
    clean_split_str = bod[0].replace('\r','').split('\n')
    final_dict = {   
                     'ДопИнформация': '',
                     'ID': '',
                     'Регион': '',
                     'Комнатность': '',
                     'Срок сдачи': '',
                     'Бюджет': '',
                     'Способ покупки': '',
                     'Телефон': '',
                     'Query': ''}
    for i in clean_split_str:
        if ': ' in i:
            param,vals = i.split(': ')
            final_dict[param] = vals[:]
        else:
            final_dict["ДопИнформация"] += i+"; "
    return final_dict

def decodes(s):
    import urllib
    s  = s.replace('%25','%') #зачем?
    s2 = urllib.parse.unquote(s)
    if '%' in s2:
        s2 = s2.replace('25','')
        s2 = urllib.parse.unquote(s2)
    return s2

# key_path = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\m2-main-cd9ed0b4e222.json'
key_path = '/home/kalmukds/m2-main-cd9ed0b4e222.json'
# key_path_extra = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\other_keys.json'
key_path_extra = '/home/kalmukds/other_keys.json'


gbq_credential = service_account.Credentials.from_service_account_file(key_path,)
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
             'https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(key_path, SCOPES)

gc = gspread.authorize(credentials)

f = open(key_path_extra, "r")
key_other = f.read()
keys = json.loads(key_other)['keys']['email']
box = Imbox('imap.yandex.com',
        username=keys['username'],
        password=keys['password'],
        ssl=True,
        ssl_context=None,
        starttls=False)

mail_dict = {
    'message_id': [],
    'sent_from': [],
    'subject': [],
    'date': [],
    'body': []
}
all_inbox_messages = box.messages(sent_from='call-back-novostroyki@service-m2.ru')
for uid, message in all_inbox_messages:
    
    if message.sent_from[0]['email'] == 'call-back-novostroyki@service-m2.ru' and 'Inhouse' in message.subject:
        
        mail_dict['message_id'].append(message.message_id)
        mail_dict['sent_from'].append(message.sent_from[0]['email'])
        mail_dict['subject'].append(message.subject)
        mail_dict['date'].append(message.parsed_date)
        mail_dict['body'].append(message.body['plain'])
        
mail_raw_pd = pandas.DataFrame(mail_dict)

mail_raw_pd['dict'] = mail_raw_pd['body'].apply(parse_body)
mail_raw_pd['PHONE'] = mail_raw_pd['dict'].apply(lambda x: x['Телефон'])
mail_raw_pd['ID'] =  mail_raw_pd['dict'].apply(lambda x: x['ID'])
mail_raw_pd['GEO'] =  mail_raw_pd['dict'].apply(lambda x: x['Регион'])
mail_raw_pd['ROOMS'] =  mail_raw_pd['dict'].apply(lambda x: x['Комнатность'])
mail_raw_pd['TIME'] =  mail_raw_pd['dict'].apply(lambda x: x['Срок сдачи'])
mail_raw_pd['COSTS'] =  mail_raw_pd['dict'].apply(lambda x: x['Бюджет'])
mail_raw_pd['Extra'] =  mail_raw_pd['dict'].apply(lambda x: x['ДопИнформация'])
mail_raw_pd['UTMS'] =  mail_raw_pd['dict'].apply(lambda x: x['Query'])
mail_raw_pd['dict_utm'] = mail_raw_pd['UTMS'].apply(decodes).apply(parse_utm)
mail_raw_pd['utm_source'] = mail_raw_pd['dict_utm'].apply(lambda x: x['utm_source'])
mail_raw_pd['utm_medium'] =  mail_raw_pd['dict_utm'].apply(lambda x: x['utm_medium'])
mail_raw_pd['utm_campaign'] =  mail_raw_pd['dict_utm'].apply(lambda x: x['utm_campaign'])
mail_raw_pd['utm_content'] =  mail_raw_pd['dict_utm'].apply(lambda x: x['utm_content'])
mail_raw_pd['utm_term'] =  mail_raw_pd['dict_utm'].apply(lambda x: x['utm_term'])
mail_raw_pd['quiz_source'] =  mail_raw_pd['dict_utm'].apply(lambda x: x['quiz_source'])

drops = [
    'sent_from',
    'message_id',
    'dict',
    'dict_utm',
    'body'
]

clean_mail = mail_raw_pd.drop(columns = drops)

# clean_mail.to_gbq(f'EXTERNAL_DATA_SOURCES.MAIL_DATA', project_id='m2-main', if_exists='replace', credentials=gbq_credential)


clk.creat_table_df(clean_mail,'external.MAIL_DATA')

clear_q = f"ALTER TABLE external.MAIL_DATA DELETE WHERE 1 = 1"
clk.get_query_results(clear_q) 

upload_multipart('external.MAIL_DATA', clean_mail)