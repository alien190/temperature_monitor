import yaml
import datetime
import sys
import time
from datetime import timedelta
import mysql.connector
import board
import adafruit_dht


class MeasurmentSaver(object):
    
    def __init__(self, user, password):
        self.user = user
        self.password = password
        self.mydb = None
     
    def __enter__(self):
        try:
            self.mydb = mysql.connector.connect(host='localhost',
                                       user=self.user,
                                       password=self.password,
                                       database = 'monitor')
            print('Connected to DB')
        except Exception as error:
            print(error)     
        return self.save
 
    def __exit__(self, *args):
        if self.mydb == None:
            print('Error! Can not close connection to DB')
            return
        
        self.mydb.close() 
        print('DB connection is closed')

    def save(self, sensor_id: int, date: datetime, temperature: float, humidity: float):
        print('    date: {}'.format(date)) 
        m1 = datetime.datetime(year=date.year, month=date.month, day=date.day, hour=date.hour, minute=date.minute)
        print(' M1 date: {} ({})'.format(m1, m1.timestamp()))
        m5 = datetime.datetime(year=date.year, month=date.month, day=date.day, hour=date.hour, minute= (date.minute // 5) * 5)
        print(' M5 date: {} ({})'.format(m5, m5.timestamp()))
        m15 = datetime.datetime(year=date.year, month=date.month, day=date.day, hour=date.hour, minute= (date.minute // 15) * 15)
        print('M15 date: {} ({})'.format(m15, m15.timestamp()))
        m30 = datetime.datetime(year=date.year, month=date.month, day=date.day, hour=date.hour, minute= (date.minute // 30) * 30)
        print('M30 date: {} ({})'.format(m30, m30.timestamp()))
        h1 = datetime.datetime(year=date.year, month=date.month, day=date.day, hour=date.hour)
        print(' H1 date: {} ({})'.format(h1, h1.timestamp()))
        h4 = datetime.datetime(year=date.year, month=date.month, day=date.day, hour=(date.hour // 4) * 4)
        print(' H4 date: {} ({})'.format(h4, h4.timestamp()))
        d1 = datetime.datetime(year=date.year, month=date.month, day=date.day)
        print(' D1 date: {} ({})'.format(d1, d1.timestamp()))
        print('Temperature: {}C, humidity: {}%'.format(temperature, humidity))

        if self.mydb == None:
            print('Error! Can not store to DB')
            return

        mycursor = self.mydb.cursor()

        sql = """INSERT INTO measurings 
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

        print('-------------------------------------------------------------------------')

def exit_program():
    print("Exiting the program...")
    sys.exit(0)

def get_gpio(gpio_id):
    if gpio_id == 4: return board.D4

def main():

    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    try:
        db_user = config['db_user']
        db_password = config['db_password']
        sensor_id = config['sensor_id']
        gpio_id = config['gpio_id']
        sleep_seconds = config['sleep_seconds']
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
            print(error.args[0])
            time.sleep(sleep_seconds)
            continue
        except Exception as error:
            exit_program()


        if last_stored_date == None or date - last_stored_date >= timedelta(seconds=15):
           last_stored_date = date
           with MeasurmentSaver(db_user, db_password) as save:
                save(sensor_id, date, temperature, humidity)  
    
        time.sleep(sleep_seconds)


if __name__ == "__main__":
    main()