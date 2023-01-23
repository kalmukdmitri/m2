import pandas
import os
import requests
import json
import datetime
import time
import pandas_gbq
import requests
import datetime
import json
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
import string


# key_path_extra = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\vk.json'
key_path_extra = '/home/kalmukds/vk.json'

f = open(key_path_extra, "r")
key_other = f.read()
keys = json.loads(key_other)['token']

def get_vk(token = '', method = 'ads.getStatistics',client='', params=''):
    user_id_V = f"&user_id=32785745&v=5.131&client_id={client}"
    base_url = f"https://api.vk.com/method/{method}?{token}{user_id_V}&{params}"
    metrica_answer = requests.get(base_url)
    results = json.loads(metrica_answer.text)
    return results

def campaign_to_table(campaign_stats,campaign_dict):
    tables = [] 
    for campaign in campaign_stats['response']:
        campaigns_table = []
        campaign_id = str(campaign['id'])
        campaign_name = campaign_dict[campaign_id]  
        for row in campaign['stats']:
            spent = 0.0 if 'spent'  not in row else row['spent']
            clicks = 0.0 if 'clicks'  not in row else row['clicks']
            impressions = 0.0 if 'impressions'  not in row else row['impressions']
            reach = 0.0 if 'reach'  not in row else row['reach']
            campaigns_table.append([campaign_id,
                                    campaign_name,
                                    row['day'],
                                    float(spent),
                                    impressions,
                                    clicks,
                                    reach
                                   ])
        tables.extend(campaigns_table)
    return tables


def dict_vk(vk_answer):
    dict_vk = {}
    for i in vk_answer['response']:
        url = i['link_url']

        if 'utm_source' in i['link_url']:
            tags = i['link_url'].split('?')[1].split('&')
            url = [i for i in tags if 'utm_campaign' in i][0].split('=')[1]
            domain = i['link_url'].split("/")[2]
        else:
            domain = i['link_url']
        dict_vk[i['campaign_id']] = [url,domain]
    
    return dict_vk

def get_vk_ads(token = '', method = 'ads.getAdsLayout',client = '', params='account_id=1900001464'):
    user_id_V = f"&user_id=32785745&v=5.131&client_id={client}"
    base_url = f"https://api.vk.com/method/{method}?{token}{user_id_V}&{params}"
    metrica_answer = requests.get(base_url)
    results = json.loads(metrica_answer.text)
    return results


def upload_multipart(table_name, df):
    
    dates_list = list(set(df['date']))
    dates_list.sort()
    
    for i in range(0,len(dates_list), 10):
        
        date_block = dates_list[i:i+10]
        regs_date = df[df['date'].apply(lambda x: x in date_block)]
        regs_date = regs_date.reset_index(drop=True)
        clk.insert(regs_date, table_name)
        
        

token = f'access_token={keys}'

clients = [
            {"id": 1606924159,
            "name": "Новостройки М2"},
            {"id": 1606924161,
            "name": "Вторичное жилье М2"}]



final_dfs = []
for client_data in clients:
    
    client = client_data['id']
    campaigns = get_vk(token = token, method = 'ads.getCampaigns', client=client,params= 'account_id=1900001464')
    camps_ids = ",".join([str(i['id']) for i in campaigns['response']])
    date_to = str(datetime.date.today())
    get_campaign_stats = get_vk(token = token, client=client, params = f'ids_type=campaign&date_from=2022-01-25&date_to={date_to}&period=day&account_id=1900001464&ids={camps_ids}')
    campaigns_dict = {str(i['id']):i['name'] for i in campaigns['response']}
    time.sleep(1)
    vk_table = campaign_to_table(get_campaign_stats,campaigns_dict)
    time.sleep(1)

    columns = ['cmp_id',
            'capmaign',
            'date',
            'cost',
            'impressions',
            'clicks',
            'reach']
    vk_df = pandas.DataFrame(vk_table, columns=columns)
    time.sleep(1)
    ads_contents = get_vk_ads(token = token,client=client )
    ids = ','.join([str(i['id']) for i in ads_contents['response']])
    cmp_map = {i['id']:i['campaign_id'] for i in ads_contents['response']}
    cmp_names = {i['id']: i['name'] for i in campaigns['response']}
    ads_links = {i['id']: i['link_url'] for i in ads_contents['response']}

    get_ids_stats = get_vk(token = token, client=client, params = f'ids_type=ad&date_from=2022-01-25&date_to={date_to}&period=day&account_id=1900001464&ids={ids}')

    x = get_ids_stats['response']
    end = []
    for ad in get_ids_stats['response']:
        for day in ad['stats']:
            day['id'] = ad['id']
            end.append(day)
    ads = pandas.DataFrame(end)
    if len(ads)>0:
        ads_pure = ads[['day','spent', 'impressions', 'clicks', 'reach','lead_form_sends', 'id']]
        ads_pure=ads_pure.fillna(0.0)
        ints = ['impressions', 'clicks', 'reach','lead_form_sends']
        for col in ints:
            ads_pure[col] = ads_pure[col].astype(int)

        ads_pure['cmp_id'] = ads_pure['id'].apply(lambda x :cmp_map[x] )
        ads_pure['cmp_name'] = ads_pure['cmp_id'].apply(lambda x :cmp_names[x] )
        ads_pure['link'] = ads_pure['id'].apply(lambda x :ads_links[x] )

        utm_dict = {}
        for ad_id in ads_links:
            utms_dict = {'utm_source': '',
                    'utm_medium': '',
                    'utm_campaign': '',
                    'utm_content': '',
                    'utm_term': ''}
            if 'utm_' in ads_links[ad_id]:
                link_pure = ads_links[ad_id].split('?')[0]
                link_utms = ads_links[ad_id].split('?')[1]
                utms_dict_link = {i.split('=')[0]:i.split('=')[1] for i in link_utms.split('&')}
                for utm in utms_dict:
                    if utm in utms_dict_link:
                        utms_dict[utm] = utms_dict_link[utm]

            utm_dict[ad_id] = utms_dict


        ads_pure['utm_source'] = ads_pure['id'].apply(lambda x: utm_dict[x]['utm_source'])
        ads_pure['utm_medium'] = ads_pure['id'].apply(lambda x: utm_dict[x]['utm_medium'])
        
        ads_pure['utm_campaign'] = ads_pure['id'].apply(lambda x: utm_dict[x]['utm_campaign'])
        ads_pure['utm_content'] = ads_pure['id'].apply(lambda x: utm_dict[x]['utm_content'])
        ads_pure['utm_term'] = ads_pure['id'].apply(lambda x: utm_dict[x]['utm_term'])
        ads_pure['projects'] = client_data['name']
        final_dfs.append(ads_pure)
final_data = pandas.concat(final_dfs)
final_data['date'] = final_data['day'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d" ).date())

final_data['spent'] = final_data['spent'].apply(lambda x: float(x)


final_data = final_data.drop(columns = ['day']).reset_index(drop=True)

from clickhouse_py  import clickhouse_pandas

def upload_multipart(table_name, df):
    
    dates_list = list(set(df['date']))
    dates_list.sort()
    
    for i in range(0,len(dates_list), 10):
        
        date_block = dates_list[i:i+10]
        regs_date = df[df['date'].apply(lambda x: x in date_block)]
        regs_date = regs_date.reset_index(drop=True)
        clk.creat_table_df(regs_date, table_name)
        
clk  = clickhouse_pandas('external')
res  = clk.get_query_results(
    f"""
    ALTER TABLE external.VK_ADS DELETE WHERE 1=1 
    """)

upload_multipart('external.VK_ADS' , final_data)