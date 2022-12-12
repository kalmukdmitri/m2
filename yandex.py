import requests
import time
import json
import pandas 
import datetime
from clickhouse_py  import clickhouse_pandas, clickhouse_logger
clk  = clickhouse_pandas('external')

class yandex_direct:

    
    def __init__(self,client, Client_Login,token ):
    # Оптределяем аккаунт откуда бер1м данные
        self.client = client
        self.headers = { 'Content-Type': 'application/json',
                         'Authorization': f'Bearer {token}',
                         'Client-Login': Client_Login,
                         'Accept-Language': 'ru',
                         'processingMode': 'auto',
                         'returnMoneyInMicros': 'false',
                         'skipReportHeader': 'false',
                         'skipColumnHeader': 'false',
                         'skipReportSummary': 'true'}
        self.Client_Login = Client_Login
        
    def ads_get(self, campaign_list = [],page = 0):
        
        body = {'method': 'get',
                'params': {'SelectionCriteria': {'CampaignIds': campaign_list},
                'FieldNames': ['Id', 'CampaignId'],
                'TextAdFieldNames': ['Href'],
                'MobileAppAdFieldNames': ['TrackingUrl'],
                'TextImageAdFieldNames': ['Href'],
                'MobileAppImageAdFieldNames': ['TrackingUrl'],
                'TextAdBuilderAdFieldNames': ['Href'],
                'MobileAppAdBuilderAdFieldNames': ['TrackingUrl'],
                'MobileAppCpcVideoAdBuilderAdFieldNames': ['TrackingUrl'],
                'CpcVideoAdBuilderAdFieldNames': ['Href'],
                'CpmBannerAdBuilderAdFieldNames': ['Href'],
                'CpmVideoAdBuilderAdFieldNames': ['Href'],
                'Page': {'Offset': 0}}}
        
        url = 'https://api.direct.yandex.com/json/v5/ads'
        print(body)
        report = requests.post(url, headers=self.headers, json=body)
        limits = report.json()['result']['LimitedBy'] if 'LimitedBy' in report.json()['result'] else 0
        print(limits)
        ads_data = report.json()['result']['Ads']
        
        while limits != 0:
            time.sleep(3)
            body['params']['Page']['Offset'] += 10000
            report = requests.post(url, headers=self.headers, json=body)
            limits = report.json()['result']['LimitedBy'] if 'LimitedBy' in report.json()['result'] else 0
            print(limits)
            ads_data.extend(report.json()['result']['Ads'])
            
        return ads_data
    
    def campaigns_get(self,page = 0):
        
        body = {
            "method": "get",
            "params": {
                "SelectionCriteria": {},
                "FieldNames": ["Id","Name","Type"],
                "TextCampaignFieldNames": ["TrackingParams"],
                "DynamicTextCampaignFieldNames": ["TrackingParams"],
                "SmartCampaignFieldNames": ["TrackingParams"],
                "Page": {'Offset': 0}
            },
        }
        
        url = 'https://api.direct.yandex.com/json/v5/campaigns'
        print(body)
        print(self.headers)
        report = requests.post(url, headers=self.headers, json=body)
        limits = report.json()['result']['LimitedBy'] if 'LimitedBy' in report.json()['result'] else 0
        print(limits)
        campaign_data = report.json()['result']['Campaigns']
        while limits != 0:
            body['params']['Page']['Offset'] += 10000
            report = requests.post(url, headers=headers, json=body)
            limits = report.json()['result']['LimitedBy'] if 'LimitedBy' in report.json()['result'] else 0
            print(limits)
            print(body)
            print(self.headers)
            campaign_data.append(report.json()['result']['Campaigns'])
            
        return campaign_data
    def ads_get_raw(self,start, end):
        
        body = {
        "method": "get",
        'params': {
            'SelectionCriteria': {
            'DateFrom': start,
            'DateTo': end},
            'FieldNames': [
            'Date',
            'CampaignId',
            'CampaignName',
            'AdGroupId',
            'CriteriaId',
            'Criteria',
            'AdId',
            'Device',
            'Clicks',
            'Cost'],
            'OrderBy': [{'Field': 'Date'}],
            'ReportName': f'Yandex_Ads_ids {start}-{end}',
            'ReportType': 'CUSTOM_REPORT',
            'DateRangeType': 'CUSTOM_DATE',
            'Format': 'TSV',
            'IncludeVAT': 'YES',
            'IncludeDiscount': 'YES'
            }
        }
        
        url = 'https://api.direct.yandex.com/json/v5/reports'
        
        report = requests.post(url, headers=self.headers, json=body)
        print(body)
        print(self.headers)
        print(report.status_code)
        print(report.text[:300])
        if report.status_code == 200:
            data = report.text
        elif report.status_code == 202 or report.status_code == 201:
            done = False
            while not done:
                
                time.sleep(15)
                report = requests.post(url, headers=self.headers, json=body)
                print(report.status_code)
                print(report.text[100:200])
                if report.status_code == 200:
                    done = True
                    data = report.text
    
        raw_tsv1 = data.split('\n')
        cols = raw_tsv1[1].split('\t')
        rows = [i.split('\t') for i in raw_tsv1[2:-1]]
        
        done_report = pandas.DataFrame(rows, columns = cols)
        print(done_report.columns)
        done_report['Clicks'] = done_report['Clicks'].astype(int)
        done_report['Cost'] = done_report['Cost'].astype(float)
            
        return done_report
    
    def get_camp_dict(self, campaigns):
        ads_dict = {str(i['Id']):i for i in campaigns}
        camps = {}
        cmp_types = ["TextCampaign", 
                     "DynamicTextCampaign",
                     "SmartCampaign"]
        for cmps_id, data in ads_dict.items():
            for types in cmp_types:
                if types in data and 'TrackingParams' in data[types] and data[types]['TrackingParams']:
                    camps[cmps_id] = data[types]['TrackingParams']
        return camps
    
    def parse_utm(self, utms):
        utms = utms.split('?')[-1]
        utms = utms.split('&')
        params = {
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
    
    def parse_ads_utms(self):
        
        campaigns = self.campaigns_get()
        campaigns_list = [i['Id'] for i in campaigns]

        ad_ids = []
        for i in range(0,len(campaigns_list),10):

            first_cmps = i 
            last_cmps = i+10
            time.sleep(3)
            print(first_cmps,last_cmps)
            ad_ids.extend(self.ads_get(campaigns_list[first_cmps:last_cmps]))
        ad_ids = [{i: str(e) if type(e) == int else e for i,e in e.items()} for e in ad_ids]
        campaigns = [{i: str(e) if type(e) == int else e for i,e in e.items()} for e in campaigns ]
        cmp_ids = self.get_camp_dict(campaigns)

        dict_ads_ids = {}
        for ad in ad_ids:

            if ('TextAd' not in ad  or '?' not in ad['TextAd']['Href']) and ad['CampaignId'] in cmp_ids:
                utms = cmp_ids[ad['CampaignId']]
            elif 'TextAd' in ad and 'Href' in ad['TextAd']:
                utms = ad['TextAd']['Href']
            else:
                utms = ''

            dict_ads_ids[ad['Id']] = utms

        return dict_ads_ids
    
    def ads_full_process(self, start, end, ad_ids):
        
        dict_ads_ids = ad_ids
        
        Ads_table = ydx.ads_get_raw(start,end)
        Ads_table['UTM'] = Ads_table['AdId'].apply(lambda x: dict_ads_ids[x] if x in dict_ads_ids else '' )
        
        rows = []
        
        cols = [
        'Date',
        'CampaignId',
        'CampaignName',
        'AdGroupId',
        'CriteriaId',
        'Criteria',
        'AdId',
        'Device',
        'Clicks',
        'Cost',
        'UTM',
        'UTM_Parsed',
        'utm_source',
        'utm_medium',
        'utm_campaign',
        'utm_content',
        'utm_term',
        ]
        for row_tech in Ads_table.iterrows():
            row = row_tech[1]
            row_new = [
                row.Date,
                row.CampaignId,
                row.CampaignName,
                row.AdGroupId,
                row.CriteriaId,
                row.Criteria,
                row.AdId,
                row.Device,
                row.Clicks,
                row.Cost,
                row.UTM]
            utms = row.UTM.replace('{campaign_id}',row.CampaignId).replace('{phrase_id}',row.CriteriaId)
            utms = utms.replace('{keyword}',row.Criteria).replace('{ad_id}',row.AdId)
            row_new.append(utms)
            utms_parsed = self.parse_utm(utms)
            row_new.append(utms_parsed['utm_source'])
            row_new.append(utms_parsed['utm_medium'])
            row_new.append(utms_parsed['utm_campaign'])
            row_new.append(utms_parsed['utm_content'])
            row_new.append(utms_parsed['utm_term'])
            rows.append(row_new)
        final_ads = pandas.DataFrame(rows,columns = cols)
        final_ads = final_ads.rename(str.lower, axis='columns')
        final_ads['client'] = self.client
        final_ads['date'] = final_ads['date'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
        return final_ads
    
key_path_extra = '/home/kalmukds/token_y.json'
print("started")
# key_path_extra = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\token_y.json'
clk  = clickhouse_pandas('external')
f = open(key_path_extra, "r")
key_other = f.read()
token = json.loads(key_other)

for client, token_log in token.items():
    
    ydx = yandex_direct(client=client, Client_Login=token_log['login'] , token=token_log['token'])
    ads_ids = ydx.parse_ads_utms()
    ads_ids = {str(i):e for i,e in ads_ids.items()}
    
    q  = f"""
    SELECT MAX(date) as l_dt FROM external.YANDEX_FULL_DATA
    where client = '{client}'
    """
    
    last_date_ct = clk.get_query_results(q)['l_dt'][0]
    
    
    start_date = last_date_ct - datetime.timedelta(days=2)
    if "1969-12-30" == str(start_date):
        start_date =datetime.date(2022,12,1)
    end_date   = datetime.datetime.today().date() - datetime.timedelta(days=1)
    dates = [str(start_date+datetime.timedelta(days=i)) for i in range((end_date-start_date).days+1)]
    
    

    
    for date in dates:
        print(date, client)
        print('\n')

        Ads_table = ydx.ads_full_process(date,date,ads_ids)
        if len(Ads_table) > 0:
            res  = clk.get_query_results(
                f"""
                ALTER TABLE external.YANDEX_FULL_DATA DELETE WHERE date = '{date}'
                and client = '{client}'
        """)
            clk.insert(Ads_table, 'external.YANDEX_FULL_DATA')