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
key_path = '/home/kalmukds/m2-main-cd9ed0b4e222.json'

gbq_credential = service_account.Credentials.from_service_account_file(key_path,)
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
             'https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(key_path, SCOPES)

gc = gspread.authorize(credentials)


clk  = clickhouse_pandas('ga')

q = f"""
SELECT *
FROM appsflyer.vk_leads
"""

wewe = clk.get_query_results(q)
sales = wewe

def parse_json_answers(answers):
    ans = json.loads(answers.replace("\'", "\""))
    ans = {i['key']:i for i in ans}
    return ans
def sheet_ready(df_r):
    for i in df_r:
        df_r[i]= df_r[i].astype(str)
    rows  = [list(df_r.columns)]
    for i in df_r.itertuples():
        ls=list(i)[1:]
        rows.append(ls)
    return rows

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
vk_df['timestamp'] = vk_df['timestamp'].apply(lambda x: str(datetime.datetime.fromtimestamp(float(x)))[:] if x else '-' )
vk_df['phone_number'] = vk_df['phone_number'].apply(lambda x: ''.join([i for i in x if i in '1234567890']))
vk_df['phone_number'] = vk_df['phone_number'].apply(lambda x: '79'+x[2:]  if x[:2]=='89' else x )
vk_df = vk_df.sort_values('timestamp').reset_index(drop=True)
vk_df_new = vk_df[vk_df['timestamp'] != '-']
sh = gc.open_by_key("1bvHgVst-i1-xRu6xAnAo8t2xKpV_9Fkmm_j__T2nqhU")
wk = sh.worksheet('Лиды ВК ААА')
g_clop=sheet_ready(vk_df_new)
wk.update('A1',g_clop)