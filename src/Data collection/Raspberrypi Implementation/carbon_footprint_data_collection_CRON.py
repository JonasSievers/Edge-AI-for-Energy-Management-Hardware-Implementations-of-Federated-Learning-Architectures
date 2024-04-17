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
from smsapi import id_smsapi, pw_smsapi
from carbon_values import co2_dict
import logging


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



df_data = pd.DataFrame()

DATA_COLLECTION_DAY = 10 #(1 is current day, 2 is today and yesterday)
DATA_COLLECTION_CHECK_LOCAL = True
HTTP_MAX_RETRIES = 3
HTTP_PAUSE_TIME = 15

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

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logging.debug(f"Data folder is {DATA_FOLDER}")

# try:
#     os.remove(DATA_FOLDER+"/carbon_footprint_data.csv")
# except:
#     pass

#   ______  _  _         _         _                      _  _                  _                  _    
#  |  ____|(_)| |       (_)       | |                    (_)| |                | |                | |   
#  | |__    _ | |  ___   _  _ __  | |_  ___   __ _  _ __  _ | |_  _   _    ___ | |__    ___   ___ | | __
#  |  __|  | || | / _ \ | || '_ \ | __|/ _ \ / _` || '__|| || __|| | | |  / __|| '_ \  / _ \ / __|| |/ /
#  | |     | || ||  __/ | || | | || |_|  __/| (_| || |   | || |_ | |_| | | (__ | | | ||  __/| (__ |   < 
#  |_|     |_||_| \___| |_||_| |_| \__|\___| \__, ||_|   |_| \__| \__, |  \___||_| |_| \___| \___||_|\_\
#                                             __/ |                __/ |                                
#                                            |___/                |___/                                 


newFile = False # Needed to create header in the new file

if not os.path.exists(DATA_FOLDER+"/carbon_footprint_data.csv"):
    logging.debug("carbon_footprint_data doesn't exist")
    with open(DATA_FOLDER+"/carbon_footprint_data.csv", 'w') as f:
        logging.debug("carbon_footprint_data file created")
        pass  # This creates an empty file

    logging.info(f"File does not exist, no local data available, carbon_footprint_data.csv created successfully.")
    dates_not_in_dataframe = formatted_dates
    newFile = True #Header will be needed because new file is empty
else:
    logging.info(f"File carbon_footprint_data already exists, skipping file creation, reading file")

    try:
        df_data = pd.read_csv(DATA_FOLDER+"/carbon_footprint_data.csv", header=0, sep=";")
    except FileNotFoundError as e:
        logging.error(f"File not found: {e.filename}")
        sys.exit(1)
    except pd.errors.ParserError as e:
        logging.error(f"Error parsing CSV: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        sys.exit(1)
    
    
    
    # List to store dates not in the DataFrame
    dates_not_in_dataframe = []

    # Check if DataFrame has debugdata
    if not df_data.empty:
        df_data['start_datetime'] = pd.to_datetime(df_data['start_datetime'])


        # Delete all data from today
        df_data = df_data[df_data['start_datetime'].dt.date != current_date]
        df_data.reset_index(drop=True, inplace=True)
        
        # Iterate through formatted dates
        for date in formatted_dates:
            logging.debug(f"Date in formatted_date is {date}")
            # Check if the date exists in the DataFrame
            if not df_data['start_datetime'].dt.strftime("%d.%m.%Y").isin([date]).any():
                logging.debug(f"{date} - No data found for date {date}, appending to dates_not_in_dataframe")
                dates_not_in_dataframe.append(date)
            else:
                local_df = df_data[df_data['start_datetime'].dt.strftime("%d.%m.%Y") == date]
                # Counting NaN values in each column
                nan_counts = local_df.isna().sum()
                # Counting columns with NaN values
                columns_with_nan = nan_counts[nan_counts > 0].count()
                logging.debug(f"{date} - Found {columns_with_nan} columns with NaN values")
                # If more than half of the columns have NaN values, add this date to the dates to be downloaded
                if columns_with_nan > (df_data.shape[1]/2):
                    logging.debug(f"{date} - More than half of the columns have NaN values, adding this date to the dates to be downloaded, removing data of {date}")
                    dates_not_in_dataframe.append(date)
                    #Remove all data of that date
                    df_data = df_data[df_data['start_datetime'].dt.strftime("%d.%m.%Y") != date]
                    d=1
                    d=2
        df_data= df_data.sort_values(by='start_datetime', ascending=True)
        df_data.to_csv(DATA_FOLDER+"/carbon_footprint_data.csv", sep=';', header=True,index=False)                     
    else:
        # If DataFrame is empty, all dates need to be downloaded
        dates_not_in_dataframe = formatted_dates



# Print the dates not in the DataFrame
logging.debug(f"Dates not in the DataFrame: {dates_not_in_dataframe}")

#    _____        _                                 _         _         
#   / ____|      | |                               | |       | |        
#  | |  __   ___ | |_   _ __    ___ __      __   __| |  __ _ | |_  __ _ 
#  | | |_ | / _ \| __| | '_ \  / _ \\ \ /\ / /  / _` | / _` || __|/ _` |
#  | |__| ||  __/| |_  | | | ||  __/ \ V  V /  | (_| || (_| || |_| (_| |
#   \_____| \___| \__| |_| |_| \___|  \_/\_/    \__,_| \__,_| \__|\__,_|


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

                    df.to_csv(DATA_FOLDER+"/tempdata.csv", sep=';', header=True,index=False)
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


#   _____          _                   _                      _               
#  |  __ \        | |                 | |                    (_)              
#  | |  | |  __ _ | |_  __ _      ___ | |  ___   __ _  _ __   _  _ __    __ _ 
#  | |  | | / _` || __|/ _` |    / __|| | / _ \ / _` || '_ \ | || '_ \  / _` |
#  | |__| || (_| || |_| (_| |   | (__ | ||  __/| (_| || | | || || | | || (_| |
#  |_____/  \__,_| \__|\__,_|    \___||_| \___| \__,_||_| |_||_||_| |_| \__, |
#                                                                        __/ |
#                                                                       |___/ 

    df = pd.read_csv(DATA_FOLDER+"/tempdata.csv", sep=";",header=1)

    os.remove(DATA_FOLDER+"/tempdata.csv")
    df = df.iloc[3:]
    # Reset the index after dropping rows
    df.reset_index(drop=True, inplace=True)
    columns_to_convert = [col for col in df.columns if col != 'MTU']
    df[columns_to_convert] = df[columns_to_convert].astype(float)
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

    # Convert the "start" column to datetime format with the current date
    df['end_datetime'] = pd.to_datetime(date_to_get_object.strftime("%Y-%m-%d") + ' ' + df['end'])
    df['end_datetime'] = df['end_datetime'].dt.tz_localize('Europe/Paris')

    # Convert the datetime to Unix timestamp
    df['end_unix_timestamp'] = df['end_datetime'].astype(int) / 10**9  # Convert nanoseconds to seconds
    # print(df)
    if newFile:
        df.to_csv(DATA_FOLDER+"/carbon_footprint_data.csv", sep=';', header=True,index=False)
        newFile = False
    else:
        df.to_csv(DATA_FOLDER+"/carbon_footprint_data.csv", sep=';', header=False,index=False, mode='a')
    



#   _____   _         _    _    _               
#  |  __ \ | |       | |  | |  (_)              
#  | |__) || |  ___  | |_ | |_  _  _ __    __ _ 
#  |  ___/ | | / _ \ | __|| __|| || '_ \  / _` |
#  | |     | || (_) || |_ | |_ | || | | || (_| |
#  |_|     |_| \___/  \__| \__||_||_| |_| \__, |
#                                          __/ |
#                                         |___/ 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


df_data = pd.read_csv(DATA_FOLDER+"/carbon_footprint_data.csv", header=0, sep=";")

df_data['start_datetime'] = pd.to_datetime(df_data['start_datetime'])
df_data['end_datetime'] = pd.to_datetime(df_data['end_datetime'])

df_data= df_data.sort_values(by='start_datetime', ascending=True)

# Plotting
plt.figure(figsize=(10, 6))

for i, column in enumerate(sorted(columns)):
    plt.plot(df_data['start_datetime'], df_data[f'{column}'], label=f'{column}', linestyle=line_styles[i%3])
plt.xlabel('Date - Hour')
plt.ylabel('MW - 15 minutes')
plt.title(f'Power Generation per Production Type - Germany - DATE')
plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))

# Rotate the x-axis labels for better readability (optional)
plt.xticks(rotation=45)
plt.savefig(DATA_FOLDER+'/Power Generation per Production Type - Germany.png')

# co2 dict ist in gr/kWh
# df[energy source] ist in MW pro viertelstunde
#  Umrechnung: df[energy_source]/4 -> MWh 
#  df[energy_source]/4*1000 -> kWh


for energy_source, co2_emission in co2_dict.items():
    df_data[f"{energy_source}_co2"] = df_data[energy_source]/4*1000 * co2_emission

df_data['totalEnergy_MWh'] = df_data[list(columns)].sum(axis=1)# still in MW pro 15 minutes
df_data['totalEnergy_MWh'] = df_data['totalEnergy_MWh']/4# now in MWh
columns_to_sum_co2 = [column + '_co2' for column in columns]
df_data['totalCO2_gr'] = df_data[columns_to_sum_co2].sum(axis=1)
df_data['emission_gr_kWh'] = round(df_data['totalCO2_gr'] / (df_data['totalEnergy_MWh']*1000),3) # gr / MWh*1000 -> gr/kWh


# Plotting
plt.figure(figsize=(10, 6))


plt.plot(df_data['start_datetime'], df_data["emission_gr_kWh"], label="emission_gr_kWh")
plt.xlabel('Date - Hour')
plt.ylabel('gr/ kWh')
plt.title(f'Emissions of electricity production (CO2 gr/kWh)   - Germany - DATE')

# Rotate the x-axis labels for better readability (optional)
plt.xticks(rotation=45)

plt.savefig(DATA_FOLDER+'/Emissions of electricity production (CO2 gr.kWh)   - Germany.png')

sys.exit(0)