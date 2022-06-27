import pandas
import pandas_gbq
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import datetime
import gspread
import os
import requests 

# key_path = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\m2-main-cd9ed0b4e222.json'
key_path = '/home/web_analytics/m2-main-cd9ed0b4e222.json'


gbq_credential = service_account.Credentials.from_service_account_file(key_path,)
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
             'https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(key_path, SCOPES)

q = """
WITH
  QUIZ AS (
  SELECT
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
    utm_campaign
  FROM
    `m2-main.EXTERNAL_DATA_SOURCES.MAIL_DATA` ), 
CALLS AS (
  SELECT
extract(date from date_time) as date ,caller, sale_state ,MAX(sold_sum) as sold_sum 
FROM `m2-main.sheets.NB_ALL_CALLS`
group by 1,2,3
)

SELECT 
    ID,
    max(utm_campaign) as utm_campaign,
    max(sale_state) as sale_state,
    min(quiz_source) as quiz_source,
    min(date_lead) as date_lead
FROM QUIZ
LEFT JOIN CALLS ON  caller= PHONE 
AND QUIZ.date_lead <= CALLS.date
WHERE quiz_source = "QUIZGO"
GROUP BY 1
""" 
leads_all_doc = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
# sales_ids = leads_all_doc[leads_all_doc['utm_campaign'].apply(lambda x : type(x) == str and '%7C' in x)]
sales_ids = leads_all_doc[leads_all_doc.date_lead.dt.date >= datetime.date(2022,6,10)]
sales_ids['click_id'] = sales_ids['utm_campaign'].apply(lambda x :  x.split('%7C')[0])
sales_ids = sales_ids.drop(columns = 'utm_campaign')

q = """
SELECT 
 ID,sale_state,quiz_source,click_id,lead_sent,sale_sent
FROM `EXTERNAL_DATA_SOURCES.POSTBACKS_QUIZGO` 
"""

post_back_state = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
def post_post_back(current_sate, new_state):
    #   post new_created
    prev_ids = set(current_sate['ID'])
    new_leads_ids_send_new = new_state[new_state['ID'].apply(lambda x: x not in prev_ids)]

    # post_sales
    not_sold_old = set(current_sate[current_sate['sale_sent'] != 0]['ID'])
    new_sold_calls = new_state[new_state['sale_state'] == 'Продан']
    new_sales_ids_send_new = new_sold_calls[new_sold_calls['ID'].apply(lambda x: x not in not_sold_old)]

    new_leads_ids_send_new['lead_sent'] = 1
    new_leads_ids_send_new['sale_sent'] = 0
    
    new_state = pandas.concat([current_sate,new_leads_ids_send_new])

    if len(new_sales_ids_send_new) > 0:

        new_sales_ids_send_new['lead_sent'] = 1
        new_sales_ids_send_new['sale_sent'] = 1

        old_states = new_state[new_state['ID'].apply(lambda x: x not in set(new_sales_ids_send_new['ID']))]
        new_state = pandas.concat([old_states,new_sales_ids_send_new])


    return new_leads_ids_send_new,new_sales_ids_send_new,new_state

new_leads_ids_send_new,new_sales_ids_send_new,new_state = post_post_back(post_back_state, sales_ids)

def send_all_new_deals(new_deals):
    
    for row in new_deals.iterrows():
        clk_id = row[1]['click_id']
        q_post = f'https://tracker.profit-cube.io/19e01a1/postback?subid={clk_id}&status=lead&from=m2'    
        print(q_post)
        result = requests.post(q_post)
        print(result)
        
        
if len(new_leads_ids_send_new) > 0:   
    
    send_all_new_deals(new_leads_ids_send_new)
    

def send_sold_deals(new_deals):
    
    for row in new_deals.iterrows():
        clk_id = row[1]['click_id']
        q_post = f'https://tracker.profit-cube.io/19e01a1/postback?subid={clk_id}&status=sale&from=m2'  
        print(q_post)
        result = requests.post(q_post)
        print(result)
        
if len(new_sales_ids_send_new) > 0:   
    send_sold_deals(new_sales_ids_send_new)
    
new_state.to_gbq(f'EXTERNAL_DATA_SOURCES.POSTBACKS_QUIZGO', project_id='m2-main', if_exists='replace', credentials=gbq_credential)