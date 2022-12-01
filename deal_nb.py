import datetime
import pandas
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
import gspread

key_path = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\m2-main-cd9ed0b4e222.json'
# key_path = '/home/kalmukds/m2-main-cd9ed0b4e222.json'
gbq_credential = service_account.Credentials.from_service_account_file(key_path,)
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
             'https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(key_path, SCOPES)

gc = gspread.authorize(credentials)
sh = gc.open_by_key("1vnn2QeLq0MZwmv3gPsxmArDXVf7KZ-w_gEe-vIwJkfE")
sheets = [i.title for i in sh.worksheets() if 'Заявки' in i.title] 
calls_columns = [
 'Manager',
 'Source',
 'Date',
 'Number',
 'Name',
 'State',
 'ID_lead',
 'Rooms',
 'Price',
 'Date_of_purchase',
 'Decor',
 'Payment_method',
 'Time_to_market',
 'Place',
 'Metro',
 'Next_call_date',
 'Next_call_time',
 'Developer',
 'JK',
 'JK_type',
 'Resuld',
 'Gains_amount',
 'Comment',
 'Times_calls',
 'salebot',
 'salebot_result',
 'check_salebot']

deals = []
for i in sheets:
    wk = sh.worksheet(i)
    list_of_dicts = wk.get_all_records()
    calls_g_c = pandas.DataFrame(list_of_dicts)
    calls_g_c.columns = calls_columns
    if 'Мск' in i:
        calls_g_c['city'] = 'Москва'
    else:
        calls_g_c['city'] = 'СПб'
    deals.append(calls_g_c)
final_table = pandas.concat(deals).reset_index(drop =True)
final_table = final_table[final_table['Number'] != '']
final_table['Gains_amount'] = final_table['Gains_amount'].apply(lambda x : 0 if x in ['', ' '] else x )
final_table['Date_Real'] = final_table['Date'].apply(lambda x: datetime.date(1900,1,1) if x == '' else datetime.datetime.strptime(x,"%d.%m.%Y").date()) 
dates_auto_fill = []
for i in list(final_table['Date']):
    val = i
    if val == '':
        val = dates_auto_fill[-1]
    dates_auto_fill.append(val)
final_table['Date_Autofill'] = dates_auto_fill

final_table.to_gbq('GOOGLE_SHEETS_DATA.DEALS_NB', project_id='m2-main',chunksize=20000, if_exists='replace', credentials=gbq_credential)