import yaml
import datetime
import sys
import time
from datetime import timedelta
import mysql.connector
import board
import adafruit_dht
from logger import Logger

class MeasurmentSaver(object):
    
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
        return self.save
 
    def __exit__(self, *args):
        if self.mydb == None:
            self.logger.log_error('Can not close connection to DB')
            return
        
        self.mydb.close() 
        self.logger.log_info('DB connection is closed')

    def save(self, sensor_id: int, date: datetime, temperature: float, humidity: float):
        self.logger.log_info('    date: {}'.format(date)) 
        m1 = datetime.datetime(year=date.year, month=date.month, day=date.day, hour=date.hour, minute=date.minute)
        self.logger.log_info(' M1 date: {} ({})'.format(m1, m1.timestamp()))
        m5 = datetime.datetime(year=date.year, month=date.month, day=date.day, hour=date.hour, minute= (date.minute // 5) * 5)
        self.logger.log_info(' M5 date: {} ({})'.format(m5, m5.timestamp()))
        m15 = datetime.datetime(year=date.year, month=date.month, day=date.day, hour=date.hour, minute= (date.minute // 15) * 15)
        self.logger.log_info('M15 date: {} ({})'.format(m15, m15.timestamp()))
        m30 = datetime.datetime(year=date.year, month=date.month, day=date.day, hour=date.hour, minute= (date.minute // 30) * 30)
        self.logger.log_info('M30 date: {} ({})'.format(m30, m30.timestamp()))
        h1 = datetime.datetime(year=date.year, month=date.month, day=date.day, hour=date.hour)
        self.logger.log_info(' H1 date: {} ({})'.format(h1, h1.timestamp()))
        h4 = datetime.datetime(year=date.year, month=date.month, day=date.day, hour=(date.hour // 4) * 4)
        self.logger.log_info(' H4 date: {} ({})'.format(h4, h4.timestamp()))
        d1 = datetime.datetime(year=date.year, month=date.month, day=date.day)
        self.logger.log_info(' D1 date: {} ({})'.format(d1, d1.timestamp()))
        self.logger.log_info('Temperature: {}C, humidity: {}%'.format(temperature, humidity))

        if self.mydb == None:
            self.logger.log_error('Can not store to DB')
            return

        mycursor = self.mydb.cursor()

        sql = """INSERT INTO temperatures 
                    (timestamp,
                    sensor_id,
                    m5,
                    m15,
                    m30,
                    h1,
                    h4,
                    d1,
                    temperature,
                    humidity) 
                VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s) 
                ON DUPLICATE KEY UPDATE temperature = %s, humidity = %s"""
        
        val = (m1.timestamp(), 
               sensor_id,
               m5.timestamp(),
               m15.timestamp(),
               m30.timestamp(),
               h1.timestamp(),
               h4.timestamp(),
               d1.timestamp(),
               temperature,
               humidity,
               temperature,
               humidity)
        
        mycursor.execute(sql, val)
        self.mydb.commit()

        self.logger.log_info('-------------------------------------------------------------------------')


def exit_program():
    sys.exit(0)

def get_gpio(gpio_id):
    if gpio_id == 4: return board.D4
    if gpio_id == 5: return board.D5

def main():

    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    try:
        db_user = config['db_user']
        db_password = config['db_password']
        sensor_id = config['sensor_id']
        gpio_id = config['gpio_id']
        sleep_seconds = config['sleep_seconds']
        is_debug_enabled = True if config['is_debug_enabled'] == 1 else False
        logger = Logger(filename='monitor.log', is_debug_enabled=is_debug_enabled)
    except Exception as error:
        print(error)   
        exit_program()

    last_stored_date = None

    dhtDevice = adafruit_dht.DHT22(get_gpio(gpio_id), use_pulseio=False)

    while True:
        try:
            temperature = dhtDevice.temperature
            humidity = dhtDevice.humidity
            date = datetime.datetime.now()
    
        except RuntimeError as error:
            logger.log_info(str(error.args[0]))
            time.sleep(sleep_seconds)
            continue
        except Exception as error:
            exit_program()


        if last_stored_date == None or date - last_stored_date >= timedelta(seconds=15):
           last_stored_date = date
           with MeasurmentSaver(db_user, db_password, logger) as save:
                save(sensor_id, date, temperature, humidity)  
    
        time.sleep(sleep_seconds)


if __name__ == "__main__":
    main()