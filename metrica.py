import json
import requests
import pandas
import datetime 
class metrica:
    #   Задаём ключ из файла
    
    #key_path = '/home/web_analytics/creds_metrics.json'
    key_path = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\creds_metrics.json'
    metr_id = '56439838'
    
    f = open(key_path, "r")
    key_other = f.read()
    keys = json.loads(key_other)['token']
    headers = {"Authorization": "Bearer " + keys}
    
    def __init__(self, metr_id = '56439838'):
    # Оптределяем аккаунт откуда бер1м данные
        self.metr_id = metr_id
    
    def date_pairs(self,date1, date2, step= 1):
    
        pairs= []
        while date2 >= date1:
            prev_date = date2 - datetime.timedelta(days=step-1) if date2 - datetime.timedelta(days=step) >= date1 else date1
            pair = [str(prev_date), str(date2)]   
            date2 -= datetime.timedelta(days=step)
            pairs.append(pair)
        pairs.reverse()
    
        return pairs
        
    def requests(self, date1,date2, metrics,dimensions, filters='',group = 'day'):
        base_url = "https://api-metrika.yandex.net/stat/v1/data/?accuracy=full&limit=100000&attribution=last"
        base_url +=f'&group={group}'
        base_url +=f'&id={metrica.metr_id}'
        base_url +=f'&dimensions={dimensions}'
        base_url +=f'&metrics={metrics}'
        base_url +=f'&filters={filters}'
        base_url +=f'&date1={date1}'
        base_url +=f'&date2={date2}'

        metrica_answer = requests.get(base_url, headers=metrica.headers)
        results = json.loads(metrica_answer.text)
        return results

    def proccess_direct_data(self, row):

        proccced_row = []
        date = row['query']['date1']
        for data in row['data']:

            internal_row = [i['name'] for i in data['dimensions']]
            internal_row.extend(data['metrics'])
            internal_row.append(date)
#      abs добавляем конечную дату
            proccced_row.append(internal_row)
        return proccced_row
    
    def pd_request(self,date1,date2, metrics,dimensions, filters='',group = 'day'):
        dates_list =  self.date_pairs(date1,date2)
        end_pd = pandas.DataFrame()
        for date in dates_list:
            
            response = self.requests(date[0],date[1], metrics,dimensions, filters,group)
            parsed_list = self.proccess_direct_data(response)
            columns = [i.split(':')[-1] for i in (dimensions+metrics+',date').split(',')]
            day_pd = pandas.DataFrame(parsed_list, columns = columns)
            end_pd = pandas.concat([end_pd,day_pd])
        return end_pd