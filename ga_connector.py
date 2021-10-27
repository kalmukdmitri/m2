from apiclient.discovery import build
from google.oauth2 import service_account
import datetime
import pandas


class ga_connect:
    #   Задаём ключ из файла
    key_path = '/home/web_analytics/m2-main-cd9ed0b4e222.json'
    credentials = service_account.Credentials.from_service_account_file(key_path,)
    analytics = build('analyticsreporting', 'v4', credentials=credentials)
    
    def __init__(self, viewId):
    # Оптределяем аккаунт откуда бер1м данные
        self.viewId = viewId

        
    def request(self, dates, metrics, dimetions, filters,page = 0):
        
    # Забираем сырые данные
        body={
                        'reportRequests': [
                        { "samplingLevel": "LARGE",
                        'viewId': self.viewId,
                        'dateRanges': [dates], 
                        'metrics': [metrics],
                        'dimensions': [dimetions],
                        'pageToken': str(page),
                        'pageSize': '10000',
                        'filtersExpression': filters
                        }]
                    }
        print(body)
        return ga_connect.analytics.reports().batchGet(body=body).execute()
    def report_get(self, dates, metrics, dimetions, filters):
#     Отдаём таблицу готовых данных
        report = ga_connect.request(self, dates= dates, metrics = metrics, dimetions = dimetions, filters = filters)
        if 'samplesReadCounts' not in report['reports'][0]['data'].keys() and 'isDataGolden' in report['reports'][0]['data'].keys():
            print('Golden data')
        else:
            print('Warning')
        columnHeader = report['reports'][0]['columnHeader']
        columns = columnHeader['dimensions'] + [i['name'] for i in columnHeader['metricHeader']['metricHeaderEntries']]
        if 'rows' not in report['reports'][0]['data']:
            return ([], columns)
        data = report['reports'][0]['data']['rows']
        report_lenth = report['reports'][0]['data']['rowCount']
        print(report_lenth)
        page = 0
        while report_lenth > 10000:
            page +=10000
            print(report_lenth)
            report_extra = ga_connect.request(self, dates, metrics, dimetions, filters, page = page)
            if 'samplesReadCounts' not in report['reports'][0]['data'].keys() and 'isDataGolden' in report['reports'][0]['data'].keys():
                print('Golden data')
            else:
                print('Warning')
            data += report_extra['reports'][0]['data']['rows']
            report_lenth -= 10000
        data_table = [i['dimensions'] + i['metrics'][0]['values'] for i in data]           
        return (data_table, columns)
    def report_pd(self,dates_couples, params):
#     Отдаём отчёт
        data  = []
        for i in dates_couples:
            params['dates'] =  {'startDate': f'{i[0]}', 'endDate': f'{i[1]}'}
            print(params['dates'])
            report_raw = self.report_get(**params)
            
            data.extend(report_raw[0])
        cols_e = report_raw[1]
        report = pandas.DataFrame(data, columns = cols_e)
        report.columns = [i.replace('ga:', '') for i in report.columns]
        print(len(report))
        for i in report.columns:
            ints = [
                'totalEvents',
                'uniqueEvents',
                'users',
                'sessions',
            ]
            if i in ints:
                report[i] = report[i].astype(int)
            
            if i == 'date': 
                report['date'] = report['date'].apply(lambda x: datetime.datetime.strptime(x,"%Y%m%d" ).date())
            
        return report