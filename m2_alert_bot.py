from clickhouse_py  import clickhouse_pandas, clickhouse_logger
import requests
import json

key_path_extra = '/home/kalmukds/other_keys.json'
# key_path_extra = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\other_keys.json'
f = open(key_path_extra, "r")
key_other = f.read()
keys = json.loads(key_other)

print('started')
clk  = clickhouse_pandas('ga')

data = [
    ['newMatomo.matomo_log_visit', 
     '''Select date(visit_last_action_time) as date,
    count(distinct idvisit) as matomo_sessions from newMatomo.matomo_log_visit
group by date
order by date desc
limit 5;'''],
    ['mart.SESSIONS_TABLE', '''Select date as date,
       count(distinct session_id) as ga_sessions from mart.SESSIONS_TABLE
group by date
order by date desc
limit 5;
''']
]

token_local = keys['keys']['telegam']['token']

method = "sendMessage"

chats = ['-905046807','-1001787794078']
url = "https://api.telegram.org/bot{token}/{method}".format(token=token_local, method=method)

for qer in data:
    print(qer[0])
    q = qer[1]
    table = clk.get_query_results(q)
    s_table= str(table)
    message = qer[0]+'\n'+s_table

    for chat in chats:
        data = {"chat_id": chat, "text": message}
        x = requests.post(url, data=data)