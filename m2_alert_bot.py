from clickhouse_py  import clickhouse_pandas, clickhouse_logger
import requests
import json
import mysql.connector as mysql
import pandas as pd
import datetime

key_path_extra = '/home/kalmukds/other_keys.json'
# key_path_extra = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\other_keys.json'
f = open(key_path_extra, "r")
key_other = f.read()
keys = json.loads(key_other)

print('started')
clk  = clickhouse_pandas('ga')

data = [
    ['mart.SESSIONS_TABLE', '''Select date as date,
       count(distinct session_id) as ga_sessions from mart.SESSIONS_TABLE
group by date
order by date desc
limit 10;
'''],
    ['matomo.matomo_log_visit', 
     '''Select date(visit_last_action_time) as date,
    count(distinct idvisit) as matomo_click_sessions from matomo.matomo_log_visit
group by date
order by date desc
limit 10'''],
    
]

token_local = keys['keys']['telegam']['token']

method = "sendMessage"

chats = ['-905046807','-1001787794078']
url = "https://api.telegram.org/bot{token}/{method}".format(token=token_local, method=method)

all_df = []

for qer in data:
    print(qer[0])
    q = qer[1]
    table = clk.get_query_results(q)
    all_df.append(table)




keys_mysql = keys['keys']['motomo_mysql']['pass']


def query_df(qry, iterate = False, chunk = 5000,iterations = 0):
    cnx = mysql.connect(
    user=keys_mysql['MySQL_matomo_user'],
    password=keys_mysql['MySQL_matomo_password'],
    host=keys_mysql['MySQL_matomo_host'],
    database="matomo"
    )
    cursor = cnx.cursor()
    cursor.execute(qry)
    res = cursor.fetchall()
    field_names = cursor.description
    field_names = [i[0] for i in cursor.description]
    cursor.close()
    cnx.close()
    return pd.DataFrame(res, columns = field_names)
q = '''SELECT date(visit_first_action_time) as date, count(distinct idvisit) as mysql_sessions
           FROM newMatomo.matomo_log_visit t
           group by date
order by date desc
limit 10'''

mysql_sessions = query_df(q)
click_df = all_df[0].merge(all_df[1], on='date', how='left')
full_table = mysql_sessions.merge(click_df, on='date', how='left').fillna(0)
full_table['mysql_sessions'] = full_table['mysql_sessions'].astype(int)
full_table['ga_sessions'] = full_table['ga_sessions'].astype(int)
full_table['matomo_click_sessions'] = full_table['matomo_click_sessions'].astype(int)

token_local = keys['keys']['telegam']['token']

method = "sendMessage"

chats = ['-905046807','-1001787794078']
url = "https://api.telegram.org/bot{token}/{method}".format(token=token_local, method=method)

s_table= str(full_table)
message = "Data collected on " + str(datetime.datetime.today().date())+'\n'+ s_table

for chat in chats:
    data = {"chat_id": chat, "text": message}
    x = requests.post(url, data=data)