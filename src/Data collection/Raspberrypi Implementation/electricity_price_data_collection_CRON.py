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


from datetime import datetime
import pandas as pd
import requests
import pandas as pd
import matplotlib.pyplot as plt
import requests
from bs4 import BeautifulSoup
import logging
import os
import sys
import time
#    _____  ____   _   _  ______  _____  _____ 
#   / ____|/ __ \ | \ | ||  ____||_   _|/ ____|
#  | |    | |  | ||  \| || |__     | | | |  __ 
#  | |    | |  | || . ` ||  __|    | | | | |_ |
#  | |____| |__| || |\  || |      _| |_| |__| |
#   \_____|\____/ |_| \_||_|     |_____|\_____|


SMARD_FILTER_ID = 4169
SMARD_REGION = "DE"
SMARD_RESOLUTION_TIMESTAMP = "hour"
SMARD_RESOLUTION_ELECTRICITY = "quaterhourly"

DATA_FOLDER = os.path.dirname(os.path.realpath(__file__))
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.debug(f"Data folder is {DATA_FOLDER}")

DATA_COLLECTION_DAY = 10 #(1 is current day, 2 is today and yesterday)
DATA_COLLECTION_CHECK_LOCAL = True

HTTP_MAX_RETRIES = 3
HTTP_PAUSE_TIME = 15

REMOVE_OLD_DATA = True
REMOVE_OLD_DATA_AFTER_DAYS = 30

current_time_seconds = current_datetime = datetime.now()
current_time_milliseconds = int(current_datetime.timestamp() * 1000)
print(current_time_milliseconds)



#    _____  __  __            _____   _____                 _         _         _         
#   / ____||  \/  |    /\    |  __ \ |  __ \               | |       | |       | |        
#  | (___  | \  / |   /  \   | |__) || |  | |   __ _   ___ | |_    __| |  __ _ | |_  __ _ 
#   \___ \ | |\/| |  / /\ \  |  _  / | |  | |  / _` | / _ \| __|  / _` | / _` || __|/ _` |
#   ____) || |  | | / ____ \ | | \ \ | |__| | | (_| ||  __/| |_  | (_| || (_| || |_| (_| |
#  |_____/ |_|  |_|/_/    \_\|_|  \_\|_____/   \__, | \___| \__|  \__,_| \__,_| \__|\__,_|
#                                               __/ |                                     
#                                              |___/                                      


#Get API posible timestamps (pTimestamp)

# Make a GET request to the REST API of SMARD API
requestURL_pTimestamp = f"https://www.smard.de/app/chart_data/{SMARD_FILTER_ID}/{SMARD_REGION}/index_{SMARD_RESOLUTION_TIMESTAMP}.json"
for i in range (HTTP_MAX_RETRIES):
    try:
        response_pTimestamp = requests.get(requestURL_pTimestamp)

        if response_pTimestamp.status_code == 200:
            retry = False
            # Convert JSON response to Pandas Series
            data = response_pTimestamp.json()
            df_Ptimestamps = pd.DataFrame(data)
            
            # Find the nearest lower timestamp to the current time (not taking the last timestamp, if timestamp looked for is in the past)
            nearest_timestamp = df_Ptimestamps.loc[df_Ptimestamps['timestamps'] <= current_time_milliseconds, 'timestamps'].max()
            print(f"For current time {current_time_milliseconds} ({datetime.fromtimestamp(current_time_milliseconds/1000)}) found nearest API timestamp: {nearest_timestamp} ({datetime.fromtimestamp(nearest_timestamp/1000)})\nDifference (in seconds): {int((current_time_milliseconds-nearest_timestamp)/1000)} \nDifference (in hours): {int((current_time_milliseconds-nearest_timestamp)/(1000*60))}")

        else:
            logging.error(f"Get pTimestamp - Request failed with status code: {response_pTimestamp.status_code}")
            retry = True
    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred: {e}")
        retry = True
    if not retry:
        break
    time.sleep(HTTP_PAUSE_TIME)
if retry:
    logging.error(f"No response after {HTTP_MAX_RETRIES} requests")
    sys.exit(1)



#Get Data from pTimestamp

for i in range (HTTP_MAX_RETRIES):
    try:
        requestURL_data = f"https://www.smard.de/app/chart_data/{SMARD_FILTER_ID}/{SMARD_REGION}/{SMARD_FILTER_ID}_{SMARD_REGION}_quarterhour_{nearest_timestamp}.json"
        response_data = requests.get(requestURL_data)
        # Check if the request was successful (status code 200)
        if response_data.status_code == 200:
            retry = False
            # Convert JSON response to Pandas Series
            data = response_data.json()
            series_data = data['series']
            df_data = pd.DataFrame(series_data, columns=['timestamp', 'smard_price_E_MWh'])
            # Drop rows with NaN values
            df_data = df_data.dropna()

            # Add column "real_price" with value "true" for non-NaN values
            df_data['day_Ahead'] = True
            df_data['day_Ahead'] = df_data['day_Ahead'].astype(str)
            print(df_data)
        else:
            retry = True
            print("Error:", response_pTimestamp.status_code)
    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred: {e}")
        retry = True
    if not retry:
        break
    time.sleep(HTTP_PAUSE_TIME)
if retry:
    logging.error(f"No response after {HTTP_MAX_RETRIES} requests")
    sys.exit(1)




#       __          __    _______  _______           
#       \ \        / //\ |__   __||__   __|          
#    __ _\ \  /\  / //  \   | |      | |  __ _  _ __ 
#   / _` |\ \/  \/ // /\ \  | |      | | / _` || '__|
#  | (_| | \  /\  // ____ \ | |      | || (_| || |   
#   \__,_|  \/  \//_/    \_\|_|      |_| \__,_||_|   


netzNutzung = 10.88 #cent pro kWh
steuern = 6.68 #cent pro kWh
energieAufschlag = 3/100 #3%
# gesamtpreis -> netzNutzung+steuern+ EPEX Spot DE + 3%
# ACHTUNG -> smard price ist €/MWh 

df_data['awattar_price_E_MWh'] = df_data['smard_price_E_MWh'] + 1000* (netzNutzung + steuern) / 100 + energieAufschlag * df_data['smard_price_E_MWh']
print(df_data)


#    _____  _              _  _                         _             _  __             _                    _           
#   / ____|| |            | || |                       | |           | |/ /            | |                  | |          
#  | (___  | |_  __ _   __| || |_ __      __ ___  _ __ | | __ ___    | ' /  __ _  _ __ | | ___  _ __  _   _ | |__    ___ 
#   \___ \ | __|/ _` | / _` || __|\ \ /\ / // _ \| '__|| |/ // _ \   |  <  / _` || '__|| |/ __|| '__|| | | || '_ \  / _ \
#   ____) || |_| (_| || (_| || |_  \ V  V /|  __/| |   |   <|  __/   | . \| (_| || |   | |\__ \| |   | |_| || | | ||  __/
#  |_____/  \__|\__,_| \__,_| \__|  \_/\_/  \___||_|   |_|\_\\___|   |_|\_\\__,_||_|   |_||___/|_|    \__,_||_| |_| \___|
karlsruhe_price = 0
grundpreis_karlsruhe = 16.18 # € pro Monat

for i in range (HTTP_MAX_RETRIES):
    try:
        url = "https://www.stadtwerke-karlsruhe.de/de/pk/strom/tarifberater-strom.php?persons=4500&jahresverbrauch=4500&plz=76133+Karlsruhe&bestandskunde=0&onlinekommunikation=1"
        response_data = requests.get(url)
        # Check if the request was successful (status code 200)
        if response_data.status_code == 200:
            retry = False
            # Parse the HTML content using BeautifulSoup
            soup = BeautifulSoup(response_data.content, 'html.parser')
            
            # Find all tables in the HTML
            tables = soup.find_all('table')
            
            # Search for the table that starts with "Arbeitspreis"
            desired_table = None
            for table in tables:
                # Find the first row in the table
                first_row = table.find('tr')
                if first_row and first_row.text.strip().startswith("Arbeitspreis"):
                    desired_table = table
                    break
            
            if desired_table:
                # Process the table as needed
                # For example, you can extract data from the table and create a Pandas DataFrame
                
                # Here is a sample code to print the table's HTML content
                # print(desired_table)
                # Convert the HTML table to a Pandas DataFrame
                df = pd.read_html(str(desired_table))[0]
                df.drop(df.columns[2], axis=1, inplace=True)
                # Print the DataFrame
                # print(df)
                value_in_cents = df.iloc[0, 1] /100 # cents pro kWh
                print(f"Karlsruhe Stadtwerke Preis {value_in_cents} Cents pro kWh")
                karlsruhe_price = value_in_cents / 100 *1000
                print(f"Karlsruhe Stadtwerke Preis {karlsruhe_price} € pro MWh")

                df_data["stadtWerkeKarlsruhe_E_MWh"] = karlsruhe_price
                

            else:
                retry = True
                print("No table starting with 'Arbeitspreis' found.")
        else:
            retry = True
            print("Error:", response_pTimestamp.status_code)
    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred: {e}")
        retry = True
    if not retry:
        break
    time.sleep(HTTP_PAUSE_TIME)
if retry:
    logging.error(f"No response after {HTTP_MAX_RETRIES} requests")
    sys.exit(1)





#   _______  _  _      _                 
#  |__   __|(_)| |    | |                
#     | |    _ | |__  | |__    ___  _ __ 
#     | |   | || '_ \ | '_ \  / _ \| '__|
#     | |   | || |_) || |_) ||  __/| |   
#     |_|   |_||_.__/ |_.__/  \___||_|   


#Grundpreis 
grundpreis_tibber = 15.73 # €/Monat
steuerUndAbgaben = 19.43 # cents pro kWh

df_data['tibber_price_E_MWh'] = df_data['smard_price_E_MWh'] + (steuerUndAbgaben /100 *1000)



#   _____   _         _    _    _               
#  |  __ \ | |       | |  | |  (_)              
#  | |__) || |  ___  | |_ | |_  _  _ __    __ _ 
#  |  ___/ | | / _ \ | __|| __|| || '_ \  / _` |
#  | |     | || (_) || |_ | |_ | || | | || (_| |
#  |_|     |_| \___/  \__| \__||_||_| |_| \__, |
#                                          __/ |
#                                         |___/ 


# Plotting
plt.figure(figsize=(10, 6))

plt.plot(pd.to_datetime(df_data['timestamp'], unit='ms'), df_data['smard_price_E_MWh']*100/1000, label='Boerse (Day Ahead)')
plt.plot(pd.to_datetime(df_data['timestamp'], unit='ms'), df_data['awattar_price_E_MWh']*100/1000, label='aWATTar')
plt.plot(pd.to_datetime(df_data['timestamp'], unit='ms'), df_data['stadtWerkeKarlsruhe_E_MWh']*100/1000, label='Stadtwerke Karlsruhe')
plt.plot(pd.to_datetime(df_data['timestamp'], unit='ms'), df_data['tibber_price_E_MWh']*100/1000, label='Tibber')


plt.xlabel('Timestamp')
plt.ylabel('Price - cents / kWh')
plt.title('Comparison of electricity prices - Karlsruhe 76133')
plt.legend()

plt.savefig(DATA_FOLDER+'/electricity_price.png')


df_data.insert(0,"timestamp_readable",pd.to_datetime(df_data['timestamp'], unit='ms'))
df_data.to_csv(DATA_FOLDER+"/electricity_price_data.csv", sep=';', header=True, index=False)