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

# key_path = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\m2-main-cd9ed0b4e222.json'
key_path = '/home/web_analytics/m2-main-cd9ed0b4e222.json'
# key_path_extra = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\other_keys.json'
key_path_extra = '/home/web_analytics/other_keys_(1).json'


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

clean_mail.to_gbq(f'EXTERNAL_DATA_SOURCES.MAIL_DATA', project_id='m2-main', if_exists='replace', credentials=gbq_credential)

sh = gc.open_by_key("1bvHgVst-i1-xRu6xAnAo8t2xKpV_9Fkmm_j__T2nqhU")
wk = sh.worksheet('Заявки Квиз')
g_clop=sheet_ready(clean_mail)
wk.update('A1',g_clop)


q = """
WITH
  QUIZ AS (
SELECT * FROM 
(SELECT
    subject,
    date AS datetime,
    extract(date from date) as date_lead,
    date,
    PHONE,
    ID,
    GEO,
    ROOMS,
    TIME,
    COSTS,
    Extra,
    quiz_source,
    utm_source,
    utm_medium,
    utm_campaign,
    utm_content,
    ROW_NUMBER() OVER (PARTITION BY PHONE ORDER BY date ASC) as first,
  FROM
    `m2-main.EXTERNAL_DATA_SOURCES.MAIL_DATA`)
  WHERE first = 1 ), 
CALLS AS (
  SELECT
extract(date from date_time) as date ,caller, sale_state ,MAX(sold_sum) as sold_sum 
FROM `m2-main.sheets.NB_ALL_CALLS`
group by 1,2,3
)
SELECT 
QUIZ.date as date,
    PHONE,
    ID,
    GEO,
    ROOMS,
    TIME,
    COSTS,
    Extra,
    quiz_source,
    utm_source,
    utm_medium,
    utm_campaign,
    utm_content,
    MAX(sale_state) as sale_state,
    MAX(sold_sum) as sold_sum
FROM QUIZ
LEFT JOIN CALLS ON  caller= PHONE 
AND QUIZ.date_lead <= CALLS.date
WHERE sale_state = "Продан"
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
ORDER BY 1 asc
""" 
sales = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
sales['date'] = sales['date'].apply(lambda x : str(x)[:19])
sh = gc.open_by_key("1bvHgVst-i1-xRu6xAnAo8t2xKpV_9Fkmm_j__T2nqhU")

wk = sh.worksheet('Заявки Квиз c продажами')
g_clop=sheet_ready(sales)
wk.update('A1',g_clop)


q = """
SELECT * FROM `m2-main.EXTERNAL_DATA_SOURCES.VK_LEADS`
""" 
sales = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
sales['answers'] = sales['answers'].apply(parse_json_answers)

quest_cols = []
for i in sales['answers']:
    quest_cols.extend([list(i.keys()) for i in sales['answers']][0])
quest_cols = list(set(quest_cols))
needed_cols = ['lead_id','form_name','answers','timestamp']
drop_cols = [i for i in sales.columns if i not in needed_cols]
sales = sales.drop(columns = drop_cols)

end_list = []
for row in sales.iterrows():
    row = row[1]
    end_cols = list(sales.columns)
    end_cols.extend(quest_cols)
    end_dict = {i:'' for i in end_cols}
    
    for i in list(row.index):
        end_dict[i] = row[i]
    end_list.append(end_dict)
    
    for i in quest_cols:
        end_dict[i] = row.answers[i]['answer'] if i in row.answers else ''
        
vk_df = pandas.DataFrame(end_list).drop(columns = 'answers')
first_columns = ['lead_id', 'form_name', 'first_name', 'phone_number','timestamp']
rest_columns = [i for i in vk_df.columns if i not in first_columns]
new_index = first_columns + rest_columns
vk_df = vk_df.reindex(columns = new_index)
vk_df['timestamp'] = vk_df['timestamp'].apply(lambda x: str(datetime.datetime.fromtimestamp(float(x)))[:-7] if x else '-' )
vk_df['phone_number'] = vk_df['phone_number'].apply(lambda x: ''.join([i for i in x if i in '1234567890']))
vk_df['phone_number'] = vk_df['phone_number'].apply(lambda x: '79'+x[2:]  if x[:2]=='89' else x )
vk_df.to_gbq(f'EXTERNAL_DATA_SOURCES.VK_CLEAN_DATA', project_id='m2-main', if_exists='replace', credentials=gbq_credential)
vk_df = vk_df.sort_values('timestamp').reset_index(drop=True)
vk_df_new = vk_df[vk_df['timestamp'] != '-']
sh = gc.open_by_key("1bvHgVst-i1-xRu6xAnAo8t2xKpV_9Fkmm_j__T2nqhU")
wk = sh.worksheet('Лиды ВК')
g_clop=sheet_ready(vk_df_new)
wk.update('A1',g_clop)