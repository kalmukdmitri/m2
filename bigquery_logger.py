import sys
import datetime 
import pandas
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account

key_path = '/home/web_analytics/m2-main-cd9ed0b4e222.json'
gbq_credential = service_account.Credentials.from_service_account_file(key_path,)

# key_path = 'C:\\Users\\kalmukds\\NOTEBOOKs\\projects\\keys\\m2-main-cd9ed0b4e222.json'
# gbq_credential = service_account.Credentials.from_service_account_file(key_path,)

class bq_logger:
    def __init__(self, table_name):
        self.table_name = table_name 
        self.started = datetime.datetime.today()
        self.file_name = sys.argv[0] 
        self.comment = '' 
        self.errors = ''
        self.rows_recieved = 0 
        self.rows_updated = 0 
        self.data_start =  datetime.datetime.today().date()
        self.data_end =  datetime.datetime.today().date()
        
        
    def add_rows_recieved(self,rows_recieved):
        self.rows_recieved = rows_recieved
        
        
    def add_rows_updated(self,rows_updated):
        self.rows_updated = rows_updated
        
    def comment_add(self,comment_text):
        self.comment = comment_text
        
    def add_data_start(self,data_start):
        self.data_start = data_start
        
    def add_data_end(self,data_end):
        self.data_end = data_end
        
        
    def pandify(self):
        
        self.ended = datetime.datetime.today()
        columns = ['table_name',
                    'file_name',
                    'started' ,
                    'ended',
                    'data_start',
                    'data_end',
                    'rows_recieved',
                    'rows_updated',
                    'errors',
                    'comment']
        
        vals = [[self.table_name, self.file_name, self.started, self.ended, self.data_start , self.data_end,
                 self.rows_recieved, self.rows_updated, self.errors, self.comment]]
        self.log_pd = pandas.DataFrame(vals,columns= columns)
        return self.log_pd
    
    def errors_found(self, errors_text):
        
        self.errors = errors_text
        self.bq_save_log()
        
    def no_errors_found(self):
  
        self.errors = "no errors"
        self.bq_save_log()
        
    def bq_save_log(self):
        loggs = self.pandify()
        loggs.to_gbq(f'TEST_LOGGS.LOGGS', project_id='m2-main',chunksize=100, if_exists='append', credentials=gbq_credential)