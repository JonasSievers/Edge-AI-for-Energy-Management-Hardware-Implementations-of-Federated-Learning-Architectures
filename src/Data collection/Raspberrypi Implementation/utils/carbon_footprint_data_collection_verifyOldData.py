import datetime
import mysql.connector
import pandas as pd
from carbon_footprint_data_collection_getDayData import get_EnergyActualGenerationOfDay
from carbon_footprint_data_collection_insertAndRemoveData import insertDataIntoDatabase
from carbon_footprint_data_collection_calculateData import calculateCo2ForDay
import logging
import time
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
# Database connection parameters
db_username = 'user'
db_password = 'password'
db_hostname = "141.52.65.99"
db_port = 3306
db_name = 'datenbank'

DELETE_DATA_AFTER = 30 #days

connection = mysql.connector.connect(
        host=db_hostname,
        port=db_port,
        user=db_username,
        password=db_password,
        database=db_name
    )


def delete_old_data():
    logging.debug("delete_old_data - Start")
    connection = mysql.connector.connect(
        host=db_hostname,
        port=db_port,
        user=db_username,
        password=db_password,
        database=db_name
    )
    oldDataTimestamp = int(datetime.datetime.combine(
        datetime.date.today() - datetime.timedelta(days=30), 
        datetime.time.min
    ).timestamp())
    try:
        cursor = connection.cursor()
        sql_command = f"DELETE FROM carbon_footprint_rawData WHERE unix_timestamp < {oldDataTimestamp};"
        cursor.execute(sql_command)
        sql_command = f"DELETE FROM carbon_footprint_data WHERE unix_timestamp < {oldDataTimestamp};"
        cursor.execute(sql_command)
        connection.commit()  # Commit the transaction
        logging.debug("delete_old_data - Success")
    except Exception as e:
        print("Error executing:", e)
    
    finally:
        # Close cursor and connection only if they were successfully created
        try:
            if cursor:
                cursor.close()
            if connection and connection.is_connected():
                connection.close()
        except NameError:
            pass
    
def verifyDailyRoutine(rePull: bool = False):
    logging.debug("verifyDailyRoutine- Start")
    dates = [datetime.date.today() - datetime.timedelta(days=i) for i in range(31)]
    for date in dates:
        logging.debug(f"verifyDailyRoutine - Starting verifySpecificDay for {date}")
        verifySpecificDay(date,rePull)
    delete_old_data()
    


def verifySpecificDay(day: datetime.date, rePull: bool = False):
    
    try:
        # Ensure the day is a datetime.date instance
        if not isinstance(day, datetime.date):
            raise ValueError("The provided day must be a datetime.date instance.")
        connection = mysql.connector.connect(
        host=db_hostname,
        port=db_port,
        user=db_username,
        password=db_password,
        database=db_name
    )
        cursor = connection.cursor()
        
        # Create timestamps for the start and end of the day
        start_of_day_timestamp = int(datetime.datetime(day.year, day.month, day.day).timestamp())
        end_of_day_timestamp = int(datetime.datetime(day.year, day.month, day.day, 23, 59, 59).timestamp())
        
        # Construct the SQL command
        sql_command = f"SELECT COUNT(*) FROM carbon_footprint_rawData WHERE unix_timestamp BETWEEN {start_of_day_timestamp} AND {end_of_day_timestamp};"
        cursor.execute(sql_command)
        
        result = cursor.fetchone()  # Use fetchone to get the single count value
        
        if result and result[0] == 96:
            logging.debug(f"verifySpecificDay - There are exactly 96 entries for {day} in rawdata - starting data verifying")
            sql_command = f"SELECT COUNT(*) FROM carbon_footprint_data WHERE unix_timestamp BETWEEN {start_of_day_timestamp} AND {end_of_day_timestamp};"
            cursor.execute(sql_command)
            result = cursor.fetchone()
            if result and result[0] != 96:
                calculateCo2ForDay(day)
        else:
            print(f"Number of entries for the specified day: {result[0] if result else 'None'} REPULLING")
            if rePull:
                df = get_EnergyActualGenerationOfDay(day)
                insertDataIntoDatabase(day,df)
                time.sleep(1)
                calculateCo2ForDay(day)
    
    except Exception as e:
        print("Error executing:", e)
    
    finally:
        # Close cursor and connection only if they were successfully created
        try:
            if cursor:
                cursor.close()
            if connection and connection.is_connected():
                connection.close()
        except NameError:
            pass


if __name__ == "__main__":
    verifyDailyRoutine(True)
