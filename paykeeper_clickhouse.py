import json
import requests
import base64
import pandas
import datetime
import os
from clickhouse_py  import clickhouse_pandas


key_path_extra = '/home/kalmukds/pk_pass.json'
# key_path_extra = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\pk_pass.json'

clk  = clickhouse_pandas('kalmukds')


f = open(key_path_extra, "r")
key_other = json.loads(f.read())

end = str(datetime.datetime.today().date())

# ПРОДУКТ ГАРАНТИЯ
url = 'https://m2-garantiya.server.paykeeper.ru/'
q2 = f'info/payments/bydate/?start=2021-01-10&end={end}&payment_system_id[]=52&status[]=success&limit=1000'

q2 = url+q2
token = key_other['gd']
headers = {
        'Authorization':  f'Basic {token}',
        "Content-Type": "application/json"
        }

rest = requests.get(q2, headers= headers)
garantiya_data = pandas.DataFrame(rest.json())
garantiya = garantiya_data.copy()
garantiya['product'] = 'GD'

# ПРОДУКТ ПРОВЕРКА
url = 'https://m2upn.server.paykeeper.ru/'
q2 = f'info/payments/bydate/?start=2021-01-01&end={end}&payment_system_id[]=52&status[]=success&limit=1000'

q2 = url+q2
token = key_other['ck']
headers = {
        'Authorization':  f'Basic {token}',
        "Content-Type": "application/json"
        }

rest = requests.get(q2, headers= headers)
check_data = pandas.DataFrame(rest.json())
check = check_data.copy()
check['product'] = 'CK'
products = pandas.concat([check,garantiya]).reset_index(drop=True)

needed_cols =  ['orderid', 'success_datetime','product', 'pay_amount']
products = products.drop(columns = [i for i in products.columns if i not in needed_cols ])
products['date'] = products['success_datetime'].apply(lambda x: datetime.datetime.strptime(x[:10],"%Y-%m-%d" ).date())
products = products.drop(columns = ['success_datetime'])

# ПРОДУКТ ONLINE_DEAL
url = 'https://m2-online-deal-pay.server.paykeeper.ru/'
q2 = f'info/invoice/list/bydate/?status[]=paid&start_date=2021-01-01&end_date={end}&from=0&limit=200'
q2 = url+q2
pk_od_token = key_other['po']
headers2 = {
        'Authorization': f"Basic {pk_od_token}",
        "Content-Type":"Content-Type: application/x-www-form-urlencoded"
        }


rest = requests.get(q2, headers= headers2)
pokupka_data = pandas.DataFrame(rest.json())
pokupka_data = pokupka_data[pokupka_data['client_phone'].apply(lambda x : '79680980748' not in x)]
pokupka_data = pokupka_data[pokupka_data['client_email'].apply(lambda x : '79312558282' not in x)]
pokupka = pokupka_data.copy()
pokupka['product'] = 'PO'
needed_cols =  ['orderid', 'paid_datetime','product' , 'pay_amount']
pokupka = pokupka.drop(columns = [i for i in pokupka.columns if i not in needed_cols ])
pokupka['date'] = pokupka['paid_datetime'].apply(lambda x: datetime.datetime.strptime(x[:10],"%Y-%m-%d" ).date())
pokupka = pokupka.drop(columns = ['paid_datetime'])
products = pandas.concat([products,pokupka]).reset_index(drop=True)
products['date'] = pandas.to_datetime(products['date'])
products['pay_amount'] = products['pay_amount'].astype(float)


res  = clk.get_query_results(
        f"""
        ALTER TABLE kalmukds.PAYKEEPERS DELETE WHERE 1=1
        """)
for i in range(0,len(products),100):
    start = i
    if i+ 100 >len(products):
        end = len(products)
    else:
        end = start+100
    part = products.iloc[start:end,:].reset_index(drop=True)
    clk.creat_table_df(part, 'PAYKEEPERS')


needed_cols =  ['orderid', 'paid_datetime','product' , 'pay_amount' , 'client_email', 'client_phone']
pokupka_full = pokupka_data.drop(columns = [i for i in pokupka_data.columns if i not in needed_cols ])
pokupka_full['date'] = pokupka_full['paid_datetime'].apply(lambda x: datetime.datetime.strptime(x[:10],"%Y-%m-%d" ).date())
# pokupka_full['date'] = pokupka_full['paid_datetime']
def b64_hid(s):

    b = s.encode("UTF-8")
    e = base64.b64encode(b)
    s1 = e.decode("UTF-8")
    return s1

pokupka_full['client_phone'] = pokupka_full['client_phone'].apply(b64_hid)
pokupka_full['client_email'] = pokupka_full['client_email'].apply(b64_hid)
pokupka_full['pay_amount']  = pokupka_full['pay_amount'].astype(float)

res  = clk.get_query_results(
        f"""
        ALTER TABLE kalmukds.PAYKEEPERS_PO_FULL DELETE WHERE 1=1
        """)
for i in range(0,len(pokupka_full),100):
    start = i
    if i+ 100 >len(pokupka_full):
        end = len(pokupka_full)
    else:
        end = start+100
    part = pokupka_full.iloc[start:end,:].reset_index(drop=True)
    clk.creat_table_df(part, 'PAYKEEPERS_PO_FULL')
