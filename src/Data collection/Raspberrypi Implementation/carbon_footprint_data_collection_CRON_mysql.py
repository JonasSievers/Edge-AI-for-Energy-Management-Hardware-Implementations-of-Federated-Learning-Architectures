import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import pytz
import numpy as np
import matplotlib.dates as mdates
import os
import sys
from carbon_values import co2_dict
import logging
from sqlalchemy import create_engine
import mysql.connector

#    _____  ____   _   _  ______  _____  _____ 
#   / ____|/ __ \ | \ | ||  ____||_   _|/ ____|
#  | |    | |  | ||  \| || |__     | | | |  __ 
#  | |    | |  | || . ` ||  __|    | | | | |_ |
#  | |____| |__| || |\  || |      _| |_| |__| |
#   \_____|\____/ |_| \_||_|     |_____|\_____|


# Define InfluxDB connection parameters
url = "http://10.42.0.250:8086"
# Database connection parameters
db_username = 'user'
db_password = 'password'
db_hostname = "10.42.0.250:3306"
db_name = 'datenbank'

mydb = mysql.connector.connect(
    host="10.42.0.250",
    port="3306",
    user="user",
    password="password",
    database="datenbank"
)

# Initialize mysql client
cursor = mydb.cursor()
df_data = pd.DataFrame()

DATA_COLLECTION_DAY = 3 #(1 is current day, 2 is today and yesterday)
DATA_COLLECTION_CHECK_LOCAL = True

HTTP_MAX_RETRIES = 3
HTTP_PAUSE_TIME = 15

REMOVE_OLD_DATA = True
REMOVE_OLD_DATA_AFTER_DAYS = 30
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


def generateDateArray():
    current_date = datetime.today().date() # Object -> datetime.date(2024, 4, 17)
    current_date_string = current_date.strftime("%d.%m.%Y") # '17.04.2024'
    formatted_dates = [current_date_string]
    for i in range(1, DATA_COLLECTION_DAY):
        previous_date = current_date - timedelta(days=i)
        formatted_dates.append(previous_date.strftime("%d.%m.%Y"))
    return formatted_dates
# -> ['17.04.2024', '16.04.2024', '15.04.2024']


def checkDatabaseDataForDate(date_string="05.05.2024"):
    date_object = datetime.strptime(date_string, "%d.%m.%Y")
    timestamp_00_00 = int(datetime.timestamp(date_object.replace(hour=0, minute=0)))
    timestamp_23_45 = int(datetime.timestamp(date_object.replace(hour=23, minute=45)))
    print("Timestamp for 00:00 on", date_string, ":", timestamp_00_00)
    print("Timestamp for 23:45 on", date_string, ":", timestamp_23_45)

    query = f"SELECT * FROM carbon_footprint_rawData cfrd WHERE start_unix_timestamp BETWEEN {timestamp_00_00} AND {timestamp_23_45}"
    cursor.execute(query)
    result = cursor.fetchall()

    if len(result) != 96:
        raise ValueError("Expected 96 rows but found {} rows.".format(len(result)))
        # LOG ERROR AND WARN - GET NEW DATA AND DELETE OLD

    # Check if any row has more than 10 null values
    for row in result:
        if sum(x is None for x in row) > 10:
            raise ValueError("Found a row with more than 10 null values.")
        


    print("finished")

def deleteDayFromDatabase(date_string):
    logging.debug(f"Start deleteDayFromDatabase for date: {date_string}")

    try:
        date_object = datetime.strptime(date_string, "%d.%m.%Y")
        timestamp_00_00 = int(datetime.timestamp(date_object.replace(hour=0, minute=0)))
        timestamp_23_45 = int(datetime.timestamp(date_object.replace(hour=23, minute=45)))
        
        query = f"DELETE FROM carbon_footprint_rawData cfrd WHERE start_unix_timestamp BETWEEN {timestamp_00_00} AND {timestamp_23_45}"
        cursor.execute(query)
        mydb.commit()
        print(cursor.rowcount, "rows deleted")
        
    except mysql.connector.Error as error:
        print("Error:", error)
    finally:
        if 'mydb' in locals() and mydb.is_connected():
            cursor.close()
            mydb.close()
            print("MySQL connection is closed")








print(generateDateArray())
checkDatabaseDataForDate()
deleteDayFromDatabase("06.05.2024")