from ga_connector import ga_connect
from google.oauth2 import service_account
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import bigquery_logger
import datetime
import gspread
import json
import os
import pandas
import pandas_gbq

def date_pairs(date1, date2, step= 1):
    pairs= []
    while date2 >= date1:
        prev_date = date2 - datetime.timedelta(days=step-1) if date2 - datetime.timedelta(days=step) >= date1 else date1
        pair = [str(prev_date), str(date2)]   
        date2 -= datetime.timedelta(days=step)
        pairs.append(pair)
    pairs.reverse()
    return pairs

def decodes(s):
    import urllib
    s  = s.replace('%25','%')
    s2 = urllib.parse.unquote(s)
    if '%' in s2:
        s2 = s2.replace('25','')
        s2 = urllib.parse.unquote(s2)
    return s2

key_path = '/home/web_analytics/m2-main-cd9ed0b4e222.json'
gbq_credential = service_account.Credentials.from_service_account_file(key_path,)

table_log_BIG = bigquery_logger.bq_logger('UA_REPORTS - ALL_TABLES')
ROWS_ALL_UPDATED = 0

try:
    # Начало лога и начало отсчёта выполнения 
    table_log = bigquery_logger.bq_logger('UA_REPORTS.UA_TRAFIC_FULL')

    q = """SELECT  MAX(DATE) as date FROM `m2-main.UA_REPORTS.UA_TRAFIC_FULL` """
    last_dt = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
    start = datetime.datetime.strptime(last_dt['date'][0],"%Y-%m-%d" ).date() + datetime.timedelta(days=1)
    end =  datetime.datetime.today().date() - datetime.timedelta(days=1)
    dates_couples = date_pairs(start, end)

    # Логируем отчётные периоды отчёты
    table_log.add_data_start(str(end))
    table_log.add_data_end(str(start))

    ga_conc = ga_connect('208464364')

    try:

        if dates_couples != []:

            params = {'dimetions':  [{'name': 'ga:date'},
                                     {'name': 'ga:landingpagepath'},
                                     {'name': 'ga:dimension1'},
                                     {'name': 'ga:dimension4'},
                                     {'name': 'ga:sourcemedium'},                         
                                     {'name': 'ga:campaign'},
                                     {'name': 'ga:adContent'},
                                     {'name': 'ga:keyword'},
                                     {'name': 'ga:deviceCategory'}      

                                    ],
                    'metrics':[{'expression': 'ga:users'}
                              ],

                    'filters': ''}

            all_traf_new = ga_conc.report_pd(dates_couples,params)

            # Логируем полученые данные
            table_log.add_rows_recieved(len(all_traf_new))

            all_traf_new['source'] = all_traf_new['sourcemedium'].apply(lambda x : x.split(' / ')[0])
            all_traf_new['medium'] = all_traf_new['sourcemedium'].apply(lambda x : x.split(' / ')[1])
            all_traf_new = all_traf_new.drop(columns = ['sourcemedium', 'users'])
            all_traf_new['date'] = all_traf_new['date'].astype(str)
            all_traf_new['keyword'] = all_traf_new['keyword'].apply(decodes)

            all_traf_new.to_gbq(f'UA_REPORTS.UA_TRAFIC_FULL', project_id='m2-main',chunksize=10000, if_exists='append', credentials=gbq_credential)
            # Логируем загружение данные

            table_log.add_rows_updated(len(all_traf_new))
            ROWS_ALL_UPDATED += len(all_traf_new)
        else:
            table_log.comment_add('no new data')
        # Логируем успешное выполнение 
        table_log.no_errors_found()
    except:
        # Логируем ошибку
        table_log.errors_found(str(sys.exc_info()[1]))


    # Начало лога и начало отсчёта выполнения 
    table_log = bigquery_logger.bq_logger('UA_REPORTS.UA_ALL_CLOPS')

    q = """SELECT  MAX(DATE) as date FROM `m2-main.UA_REPORTS.UA_ALL_CLOPS` """
    last_dt = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
    start = datetime.datetime.strptime(last_dt['date'][0],"%Y-%m-%d" ).date() + datetime.timedelta(days=1)
    end =  datetime.datetime.today().date() - datetime.timedelta(days=1)
    dates_couples = date_pairs(start, end)

    # Логируем отчётные периоды отчёты
    table_log.add_data_start(str(end))
    table_log.add_data_end(str(start))

    try:

        if dates_couples != []:

            filtr = 'ga:eventlabel=~Phone'
            params = {'dimetions':  [{'name': 'ga:date'},
                                     {'name': 'ga:dimension1'},
                                     {'name': 'ga:dimension4'},
                                     {'name': 'ga:pagepath'},
                                     {'name': 'ga:eventlabel'},            
                                     {'name': 'ga:eventAction'},
                                     {'name': 'ga:eventCategory'}
                                    ],  
                    'metrics':[{'expression': 'ga:users'},
                               {'expression': 'ga:totalEvents'},
                               {'expression': 'ga:uniqueEvents'}
                              ],

                    'filters': filtr
                    }

            clops = ga_conc.report_pd(dates_couples,params)

            # Логируем полученые данные
            table_log.add_rows_recieved(len(clops))

            clops['date'] = clops['date'].astype(str)
            clops.to_gbq(f'UA_REPORTS.UA_ALL_CLOPS', project_id='m2-main',chunksize=20000, if_exists='append', credentials=gbq_credential)
            table_log.add_rows_updated(len(clops))
            ROWS_ALL_UPDATED += len(all_traf_new)
        else:
            table_log.comment_add('no new data')
        # Логируем успешное выполнение 
        table_log.no_errors_found()
    except:
        # Логируем ошибку
        table_log.errors_found(str(sys.exc_info()[1]))

    table_log = bigquery_logger.bq_logger('UA_REPORTS.VISITS_DT')

    q = """SELECT  MAX(dateHourMinute) as date FROM `m2-main.UA_REPORTS.VISITS_DT` """
    last_dt = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
    start = last_dt['date'][0].date() + datetime.timedelta(days=1)
    end =  datetime.datetime.today().date() - datetime.timedelta(days=1)
    dates_couples = date_pairs(start, end)

    # Логируем отчётные периоды отчёты
    table_log.add_data_start(str(end))
    table_log.add_data_end(str(start))


    try:

        if dates_couples != []:

            params = {'dimetions':  [{'name': 'ga:dateHourMinute'},
                                     {'name': 'ga:dimension4'}     

                                    ],
                    'metrics':[{'expression': 'ga:visits'}
                              ],

                    'filters': ''
                    }
            visit_start = ga_conc.report_pd(dates_couples,params)
            table_log.add_rows_recieved(len(visit_start))
            visit_start['dateHourMinute'] = visit_start['dateHourMinute'].apply(lambda x: datetime.datetime.strptime(x,"%Y%m%d%H%M"))
            visit_start.to_gbq(f'UA_REPORTS.VISITS_DT', project_id='m2-main',chunksize=20000, if_exists='append', credentials=gbq_credential)
            table_log.add_rows_updated(len(visit_start))
            ROWS_ALL_UPDATED += len(all_traf_new)
        else:
            table_log.comment_add('no new data')
        # Логируем успешное выполнение 
        table_log.no_errors_found()
    except:
        # Логируем ошибку
        table_log.errors_found(str(sys.exc_info()[1]))

    table_log = bigquery_logger.bq_logger('UA_REPORTS.USERS')
    q = """SELECT  MAX(DATE) as date FROM `m2-main.UA_REPORTS.USERS` """
    last_dt = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
    start = datetime.datetime.strptime(last_dt['date'][0],"%Y-%m-%d" ).date() + datetime.timedelta(days=1)
    end =  datetime.datetime.today().date() - datetime.timedelta(days=1)
    dates_couples = date_pairs(start, end)

    # Логируем отчётные периоды отчёты
    table_log.add_data_start(str(end))
    table_log.add_data_end(str(start))

    try:

        if dates_couples != []:

            filtr = ''
            params = {'dimetions':  [{'name': 'ga:date'},
                                     {'name': 'ga:dimension1'},
                                     {'name': 'ga:dimension2'},
                                     {'name': 'ga:dimension3'}],
                    'metrics' : [{'expression': 'ga:users'}],
                    'filters' : filtr}

            USERS = ga_conc.report_pd(dates_couples,params)
            table_log.add_rows_recieved(len(USERS))
            USERS.to_gbq(f'UA_REPORTS.USERS', project_id='m2-main',chunksize=20000, if_exists='append', credentials=gbq_credential)
            table_log.add_rows_updated(len(USERS))
        else:
            table_log.comment_add('no new data')
        # Логируем успешное выполнение 
        table_log.no_errors_found()
    except:
        # Логируем ошибку
        table_log.errors_found(str(sys.exc_info()[1]))


    table_log = bigquery_logger.bq_logger('UA_REPORTS.RAW_EVENTS')
    q = """SELECT  MAX(dateHourMinute) as date FROM `m2-main.UA_REPORTS.RAW_EVENTS` """
    last_dt = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
    start = last_dt['date'][0].date() + datetime.timedelta(days=1)
    end =  datetime.datetime.today().date() - datetime.timedelta(days=1)
    dates_couples = date_pairs(start, end)

    # Логируем отчётные периоды отчёты
    table_log.add_data_start(str(end))
    table_log.add_data_end(str(start))

    try:

        if dates_couples != []:

            filtr = 'ga:eventlabel!~View'
            params = {'dimetions':  [{'name': 'ga:dateHourMinute'},
                                     {'name': 'ga:dimension4'},
                                     {'name': 'ga:pagepath'},
                                     {'name': 'ga:eventlabel'},            
                                     {'name': 'ga:eventAction'},
                                     {'name': 'ga:eventCategory'}
                                    ],  
                    'metrics':[{'expression': 'ga:users'},
                               {'expression': 'ga:totalEvents'},
                               {'expression': 'ga:uniqueEvents'}
                              ],

                    'filters': filtr
                    }
            ALL_EVENTS = ga_conc.report_pd(dates_couples,params)
            table_log.add_rows_recieved(len(ALL_EVENTS))
            ALL_EVENTS['dateHourMinute'] = ALL_EVENTS['dateHourMinute'].apply(lambda x: datetime.datetime.strptime(x,"%Y%m%d%H%M"))
            ALL_EVENTS.to_gbq(f'UA_REPORTS.RAW_EVENTS', project_id='m2-main',chunksize=20000, if_exists='append', credentials=gbq_credential)
            table_log.add_rows_updated(len(ALL_EVENTS))
        else:
            table_log.comment_add('no new data')
        # Логируем успешное выполнение 
        table_log.no_errors_found()
    except:
        # Логируем ошибку
        table_log.errors_found(str(sys.exc_info()[1]))

    table_log = bigquery_logger.bq_logger('UA_REPORTS.PAGE_VIEWS')    
    q = """SELECT  MAX(dateHourMinute) as date FROM `m2-main.UA_REPORTS.PAGE_VIEWS` """
    last_dt = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
    start = last_dt['date'][0].date() + datetime.timedelta(days=1)

    end =  datetime.datetime.today().date() - datetime.timedelta(days=1)
    dates_couples = date_pairs(start, end)

    # Логируем отчётные периоды отчёты
    table_log.add_data_start(str(end))
    table_log.add_data_end(str(start))

    try:

        if dates_couples != []:

            filtr = ''
            params = {'dimetions':  [{'name': 'ga:dateHourMinute'},
                                     {'name': 'ga:dimension4'},
                                     {'name': 'ga:pagepath'}
                                    ],  
                    'metrics':[{'expression': 'ga:pageviews'}
                              ],

                    'filters': filtr
                    }
            PAGES = ga_conc.report_pd(dates_couples,params)
            table_log.add_rows_recieved(len(PAGES))
            PAGES['dateHourMinute'] = PAGES['dateHourMinute'].apply(lambda x: datetime.datetime.strptime(x,"%Y%m%d%H%M"))
            PAGES.to_gbq(f'UA_REPORTS.PAGE_VIEWS', project_id='m2-main',chunksize=20000, if_exists='append', credentials=gbq_credential)
            table_log.add_rows_updated(len(PAGES))
            ROWS_ALL_UPDATED += len(all_traf_new)
        else:
            table_log.comment_add('no new data')
        # Логируем успешное выполнение 
        table_log.no_errors_found()
    except:
        table_log.errors_found(str(sys.exc_info()[1]))
    table_log.add_rows_updated(ROWS_ALL_UPDATED)
    table_log.no_errors_found()
except:
    table_log.errors_found(str(sys.exc_info()[1]))