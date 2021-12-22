import json
import requests
import base64
import pandas
import datetime
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
import os

key_path = '/home/web_analytics/m2-main-cd9ed0b4e222.json'
key_path_extra = '/home/web_analytics/pk_pass.json'

gbq_credential = service_account.Credentials.from_service_account_file(key_path,)

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
garantiya = pandas.DataFrame(rest.json())
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
check = pandas.DataFrame(rest.json())
check['product'] = 'CK'
products = pandas.concat([check,garantiya]).reset_index(drop=True)

needed_cols =  ['orderid', 'success_datetime','product']
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
pokupka = pandas.DataFrame(rest.json())
pokupka['product'] = 'PO'
needed_cols =  ['orderid', 'paid_datetime','product']
pokupka = pokupka.drop(columns = [i for i in pokupka.columns if i not in needed_cols ])
pokupka['date'] = pokupka['paid_datetime'].apply(lambda x: datetime.datetime.strptime(x[:10],"%Y-%m-%d" ).date())
pokupka = pokupka.drop(columns = ['paid_datetime'])
products = pandas.concat([products,pokupka]).reset_index(drop=True)
products['date'] = pandas.to_datetime(products['date'])

products.to_gbq(f'EXTERNAL_DATA_SOURCES.PAYKEEPERS', project_id='m2-main', if_exists='replace', credentials=gbq_credential)