#  __     __                   _        _      ______      _  _               _     
#  \ \   / /                  (_)      | |    |  ____|    (_)| |             | |    
#   \ \_/ /__ _  _ __   _ __   _   ___ | | __ | |__  _ __  _ | |_  ___   ___ | |__  
#    \   // _` || '_ \ | '_ \ | | / __|| |/ / |  __|| '__|| || __|/ __| / __|| '_ \ 
#     | || (_| || | | || | | || || (__ |   <  | |   | |   | || |_ \__ \| (__ | | | |
#   __|________|___ |__|__ |_||___\______|\___|_|_  __|   |_| \__||___/ \___||_| |_|
#  /_ ||____  |/ _ \ | || |   |__ \  / _ \|__ \ | || |                              
#   | |    / /| | | || || |_     ) || | | |  ) || || |_                             
#   | |   / / | | | ||__   _|   / / | | | | / / |__   _|                            
#   | |  / /_ | |_| |   | | _  / /_ | |_| |/ /_    | |                              
#   |_| /_/(_) \___/    |_|(_)|____| \___/|____|   |_|                              


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

#    _____  ____   _   _  ______  _____  _____ 
#   / ____|/ __ \ | \ | ||  ____||_   _|/ ____|
#  | |    | |  | ||  \| || |__     | | | |  __ 
#  | |    | |  | || . ` ||  __|    | | | | |_ |
#  | |____| |__| || |\  || |      _| |_| |__| |
#   \_____|\____/ |_| \_||_|     |_____|\_____|
# Create Dataframe for Data
# Set max days to get and create an array of days
# Set if local data has to be used
# Set http max retries if error when getting data
# Set data folder
# Create columns dict
# Create linestyle dict
# Set logging policy

# Define InfluxDB connection parameters
url = "http://10.42.0.250:8086"
# Database connection parameters
db_username = 'user'
db_password = 'password'
db_hostname = "10.42.0.250:3306"
db_name = 'datenbank'

# Initialize mysql client
engine = create_engine(f'mysql+mysqlconnector://{db_username}:{db_password}@{db_hostname}/{db_name}')

df_data = pd.DataFrame()

DATA_COLLECTION_DAY = 3 #(1 is current day, 2 is today and yesterday)
DATA_COLLECTION_CHECK_LOCAL = True

HTTP_MAX_RETRIES = 3
HTTP_PAUSE_TIME = 15

REMOVE_OLD_DATA = True
REMOVE_OLD_DATA_AFTER_DAYS = 30

# DATA_FOLDER = "../../data"
# DATA_FOLDER = os.getcwd()
DATA_FOLDER = os.path.dirname(os.path.realpath(__file__))


columns = {
    'Biomass',
    'Fossil Brown coal/Lignite',
    'Fossil Coal-derived gas',
    'Fossil Gas',
    'Fossil Hard coal',
    'Fossil Oil',
    'Fossil Oil shale',
    'Fossil Peat',
    'Geothermal',
    'Hydro Pumped Storage',
    'Hydro Pumped Storage.1',
    'Hydro Run-of-river and poundage',
    'Hydro Water Reservoir',
    'Marine',
    'Nuclear',
    'Other',
    'Other renewable',
    'Solar',
    'Waste',
    'Wind Offshore',
    'Wind Onshore'
}
line_styles = ['-', '--', '-.', ':']


current_date = datetime.today().date() # Object -> datetime.date(2024, 4, 17)
current_date_string = current_date.strftime("%d.%m.%Y") # '17.04.2024'
formatted_dates = [current_date_string]
# Loop through the previous days and add them to the list
for i in range(1, DATA_COLLECTION_DAY):
    previous_date = current_date - timedelta(days=i)
    formatted_dates.append(previous_date.strftime("%d.%m.%Y"))
# -> ['17.04.2024', '16.04.2024', '15.04.2024']

if DATA_COLLECTION_DAY >=REMOVE_OLD_DATA_AFTER_DAYS :
    logging.error(f"Overlapping days of collection and removal {DATA_COLLECTION_DAY} >= {REMOVE_OLD_DATA_AFTER_DAYS}")
    sys.exit(1)

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logging.debug(f"Data folder is {DATA_FOLDER}")

today = datetime.today()
formatted_date = today.strftime('%d.%m.%Y')

dates_not_in_dataframe = formatted_dates


for date_to_get in dates_not_in_dataframe:
    logging.info(f"Getting data for {date_to_get}")
    retry = False
    for i in range(HTTP_MAX_RETRIES):
        logging.debug(f"{date_to_get} try number: {i}")

        # URL of the webpage containing the table
        url = f"https://transparency.entsoe.eu/generation/r2/actualGenerationPerProductionType/show?name=&defaultValue=false&viewType=TABLE&areaType=CTY&atch=false&datepicker-day-offset-select-dv-date-from_input=D&dateTime.dateTime={date_to_get}+00:00|CET|DAYTIMERANGE&dateTime.endDateTime={date_to_get}+00:00|CET|DAYTIMERANGE&area.values=CTY|10Y1001A1001A83F!CTY|10Y1001A1001A83F&productionType.values=B01&productionType.values=B02&productionType.values=B03&productionType.values=B04&productionType.values=B05&productionType.values=B06&productionType.values=B07&productionType.values=B08&productionType.values=B09&productionType.values=B10&productionType.values=B11&productionType.values=B12&productionType.values=B13&productionType.values=B14&productionType.values=B20&productionType.values=B15&productionType.values=B16&productionType.values=B17&productionType.values=B18&productionType.values=B19&dateTime.timezone=CET_CEST&dateTime.timezone_input=CET+(UTC+1)+/+CEST+(UTC+2)"
        
        logging.debug(f"{date_to_get} - Fetching from {url}")
        # Send a GET request to fetch the webpage content
        try:
            response = requests.get(url)

            if response.status_code == 200:
                logging.debug(f"{date_to_get} - ..successful")
                retry = False
                # Parse the HTML content using BeautifulSoup
                soup = BeautifulSoup(response.text, 'html.parser')

                # Find all tables in the HTML
                tables = soup.find_all('table')

                # Iterate through each table to find the one containing "Biomass"
                target_table = None
                for table in tables:
                    if "Biomass" in table.get_text():
                        target_table = table
                        break

                if target_table:
                    # Convert the HTML table to a pandas DataFrame
                    df = pd.read_html(str(target_table))[0]

                    # Replace "n/e" with NaN
                    df.replace('-', np.nan, inplace=True)
                    df.replace('n/e', np.nan, inplace=True)

                    # df.to_csv(DATA_FOLDER+"/tempdata.csv", sep=';', header=True,index=False)
                    columns_to_drop = []
                    for column in df.columns:
                        if 'Actual Consumption' in column[2]:  # Checking the second level of the multi-level header
                            columns_to_drop.append(column)

                    # Drop the columns
                    df = df.drop(columns=columns_to_drop)
                    # Only keep the second line of multi level header in dataframe
                    df.columns = df.columns.get_level_values(1)
                    df.reset_index(drop=True, inplace=True)
                    columns_to_convert = [col for col in df.columns if col != 'MTU']
                    print("Length of DataFrame:", len(df))
                    print("Length of values to assign:", len(df['MTU'].str.split(' - ', expand=True)))
                    for column in df.columns:
                        if column != 'MTU':
                            df[column] = df[column].astype(float)

                    df[['start', 'end']] = df['MTU'].str.split(' - ', expand=True)


                    # Get the current date and time in CET timezone
                    current_datetime_cet = datetime.now(pytz.timezone('Europe/Paris'))

                    # Extract the date part
                    date_to_get_object = datetime.strptime(date_to_get, "%d.%m.%Y")
                    # Convert the "start" column to datetime format with the current date
                    df['start_datetime'] = pd.to_datetime(date_to_get_object.strftime("%Y-%m-%d") + ' ' + df['start'])
                    df['start_datetime'] = df['start_datetime'].dt.tz_localize('Europe/Paris')

                    # Convert the datetime to Unix timestamp
                    df['start_unix_timestamp'] = df['start_datetime'].astype(int) / 10**9  # Convert nanoseconds to seconds
                    df.set_index('start_unix_timestamp', inplace=True)
                    # Convert the "start" column to datetime format with the current date
                    df['end_datetime'] = pd.to_datetime(date_to_get_object.strftime("%Y-%m-%d") + ' ' + df['end'])
                    df['end_datetime'] = df['end_datetime'].dt.tz_localize('Europe/Paris')

                    # Convert the datetime to Unix timestamp
                    df['end_unix_timestamp'] = df['end_datetime'].astype(int) / 10**9  # Convert nanoseconds to seconds
                    print(df)

                    df.to_sql('carbon_footprint_rawData',  con=engine, if_exists='append', index=True, index_label='start_unix_timestamp', method='multi', chunksize=1000)
                    retry = False
                    
                else:
                    logging.error(f"{date_to_get} - No table containing 'Biomass' found on the webpage.")
                    retry = True
            else:
                logging.error(f"{date_to_get} - Request failed with status code: {response.status_code}")
                retry = True
        except requests.exceptions.RequestException as e:
            logging.error(f"An error occurred: {e}")
            retry = True

        

        if not retry:
            # Exit for loop
            break
        logging.info(f"{date_to_get} - Waiting for {HTTP_PAUSE_TIME} seconds to make next request")
        time.sleep(HTTP_PAUSE_TIME)
    if retry:
        logging.error(f"Get data - {date_to_get} - After {HTTP_MAX_RETRIES} attempts to retrieve data website failed")
        sys.exit(1)