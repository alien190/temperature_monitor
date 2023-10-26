import json
import time
import yaml
from datetime import timedelta
import mysql.connector
import requests
from logger import Logger


sql_limit = 25

def request_last_uploaded_timetamp(remote_host: str):
    request = requests.get("http://" + remote_host + "/get_data/last_timestamp/")
    if(request.status_code != 200): 
        raise Exception('Error! Can not get last_uploaded_timestamp. Status code:' + str(request.status_code))

    data = json.loads(request.text)
    return data['last_timestamp']     

def send_data(remote_host: str, data):
    data_to_send = {"measurments" : data}
    request = requests.post("http://" + remote_host + "/update_data/", json=data_to_send)
    if(request.status_code != 200): 
        raise Exception('Can not upload data. Staus code:' + str(request.status_code))
      
class MeasurmentGetter(object):
    
    def __init__(self, user, password, logger: Logger):
        self.user = user
        self.password = password
        self.mydb = None
        self.logger = logger
     
    def __enter__(self):
        try:
            self.mydb = mysql.connector.connect(host='localhost',
                                       user=self.user,
                                       password=self.password,
                                       database = 'monitor')
            self.logger.log_info('Connected to DB')
        except Exception as error:
            self.logger.log_error(str(error))     
        return self.get
 
    def __exit__(self, *args):
        if self.mydb == None:
            self.logger.log_error('Can not close connection to DB')
            return
        
        self.mydb.close() 
        self.logger.log_info('DB connection is closed')

    def get(self, last_timestamp: int):
        if self.mydb == None:
            self.logger.log_error('Can not store to DB')
            return

        mycursor = self.mydb.cursor(dictionary=True)

        sql = """SELECT * FROM measurings 
                    WHERE timestamp >= %s
                    ORDER BY timestamp ASC
                    LIMIT %s
                    """
        
        val = (last_timestamp,sql_limit)
        mycursor.execute(sql, val)
        return mycursor.fetchall()

def main():
    while(True):
        try:
            with open('config.yaml', 'r') as file:
                config = yaml.safe_load(file)

            db_user = config['db_user']
            db_password = config['db_password']
            remote_host = config['remote_host']
            is_debug_enabled = True if config['is_debug_enabled'] == 1 else False
            logger = Logger(filename='data_sender.log', is_debug_enabled=is_debug_enabled)
            
            last_timestamp = request_last_uploaded_timetamp(remote_host)

            logger.log_info(str(last_timestamp))

            with MeasurmentGetter(db_user, db_password, logger) as get:
                while(True):
                    data = get(last_timestamp)
                    send_data(remote_host, data)
                    if(len(data) == sql_limit): 
                        last_timestamp = data[sql_limit-1]['timestamp']
                        logger.log_info(str(last_timestamp))
                    else:    
                        break
                
        except Exception as error:
            logger.log_error(str(error))   

        time.sleep(300)    
        

if __name__ == "__main__":
    main()
