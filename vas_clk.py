from sqlalchemy import create_engine
from clickhouse_driver.client import Client
import datetime
import pandas
import json

# key_path_click = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\click.json'

key_path_click = '/home/kalmukds/click.json'

f = open(key_path_click, "r")
key_other = f.read()
keys = json.loads(key_other)

HOST = keys['db_host'] 
PORT = keys['db_port']
PASS = keys['db_password']
DB = keys['db_name']
USER = keys['db_user']
db_url = 'http://' + USER + ':' + PASS + '@' + HOST + ':' + PORT + '/' + DB

def upload_multipart(table_name, df):
    
    dates_list = list(set(df['date']))
    dates_list.sort()
    
    for i in range(0,len(dates_list), 10):
        
        date_block = dates_list[i:i+10]
        regs_date = df[df['date'].apply(lambda x: x in date_block)]
        regs_date = regs_date.reset_index(drop=True)
        clk.insert(regs_date, table_name)

def init_client():
    print('Init db connection on ' + db_url)
    return Client.from_url(db_url)

client = init_client()
res = client.execute('''SELECT * from monetization_registry.payment_detailed''')
cols = client.execute('''DESCRIBE TABLE monetization_registry.payment_detailed''')
cols = list(pandas.DataFrame(cols)[0])
results = pandas.DataFrame(res, columns = cols)
results['order_id'] = results['order_id'].astype(str)
results['payer_user_id'] = results['payer_user_id'].astype(str)

from clickhouse_py  import clickhouse_pandas, clickhouse_logger
results['payment_dt'] = results['payment_dt'].dt.tz_localize(None)
results['date'] = results['payment_dt'].apply(lambda x : x.date())
clk  = clickhouse_pandas('external')

q = 'ALTER TABLE external.UP_VAS_TABLE DELETE WHERE 1=1'
clk.query(q)
upload_multipart('external.UP_VAS_TABLE', results)