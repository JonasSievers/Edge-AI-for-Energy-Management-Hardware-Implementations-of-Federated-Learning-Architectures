import datetime
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine, text
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


co2_dict= {
    'Biomass': 230,
    'Fossil Brown coal/Lignite': 1137,
    'Fossil Coal-derived gas': 650,
    'Fossil Gas': 381,
    'Fossil Hard coal': 853,
    'Fossil Oil': 859,
    'Fossil Oil shale': 800,
    'Fossil Peat': 800,
    'Geothermal': 38,
    'Hydro Pumped Storage': 32,
    'Hydro Run-of-river and poundage': 32,
    'Hydro Water Reservoir': 32,
    'Marine': 24,
    'Nuclear': 11,
    'Other': 200,
    'Other renewable': 25,
    'Solar': 143,
    'Waste': 350,
    'Wind Offshore': 4,
    'Wind Onshore': 7
}
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

# Database connection parameters
db_username = 'user'
db_password = 'password'
db_hostname = "141.52.65.99"
db_port = 3306
db_name = 'datenbank'

connection = mysql.connector.connect(
        host=db_hostname,
        port=db_port,
        user=db_username,
        password=db_password,
        database=db_name
    )
# Database connection parameters
db_username = 'user'
db_password = 'password'
db_hostname = "141.52.65.99:3306"
db_name = 'datenbank'
db_table_raw_data='carbon_footprint_data' 

# Initialize MySQL client
engine = create_engine(f'mysql+mysqlconnector://{db_username}:{db_password}@{db_hostname}/{db_name}')

def removeCo2DataForDay(day: datetime.date):
    logging.debug('removeCo2DataForDay - Start')
    # Create timestamps for the start and end of the day
    start_of_day_timestamp = int(datetime.datetime(day.year, day.month, day.day).timestamp())
    end_of_day_timestamp = int(datetime.datetime(day.year, day.month, day.day, 23, 59, 59).timestamp())
    # Construct SQL command without single quotes around timestamps
    sql_command = f"DELETE FROM {db_table_raw_data} WHERE unix_timestamp BETWEEN {start_of_day_timestamp} AND {end_of_day_timestamp};"

    # Execute SQL command
    logging.debug('removeCo2DataForDay - Sending SQL command')
    connection = engine.connect()
    trans = connection.begin()
    try:
        connection.execute(text(sql_command))
        trans.commit()
        logging.debug('removeCo2DataForDay - Finished - Deleted data successfull')
    except Exception:
        trans.rollback()
        raise




def calculateCo2ForDay(day: datetime.date):
    logging.debug('calculateCo2ForDay - Start')
    try:
        # Ensure the day is a datetime.date instance
        if not isinstance(day, datetime.date):
            raise ValueError("The provided day must be a datetime.date instance.")
        
        cursor = connection.cursor()
        
        # Create timestamps for the start and end of the day
        start_of_day_timestamp = int(datetime.datetime(day.year, day.month, day.day).timestamp())
        end_of_day_timestamp = int(datetime.datetime(day.year, day.month, day.day, 23, 59, 59).timestamp())
        
        # Construct the SQL command
        sql_command = f"SELECT * FROM carbon_footprint_rawData WHERE unix_timestamp BETWEEN {start_of_day_timestamp} AND {end_of_day_timestamp};"
        cursor.execute(sql_command)
        logging.debug('calculateCo2ForDay - Execute SQL command')
        results = cursor.fetchall()
        logging.debug('calculateCo2ForDay - Found data')
        if len(results) == 0:
            logging.debug('No values found for the day')
            raise ValueError("No values for day",day)
            return
        
        df_data = pd.DataFrame(results, columns=[column[0] for column in cursor.description])
        
        # Calculate CO2 emissions for each energy source
        for energy_source, co2_emission in co2_dict.items():
            df_data[f"{energy_source}_co2"] = df_data[energy_source]/4*1000 * co2_emission

        # Calculate total energy in MWh
        df_data['totalEnergy_MWh'] = df_data[list(columns)].sum(axis=1) / 4

        # Calculate total CO2 emissions in grams
        columns_to_sum_co2 = [column + '_co2' for column in columns]
        df_data['totalCO2_gr'] = df_data[columns_to_sum_co2].sum(axis=1)

        # Calculate emission factor in grams per kWh
        df_data['emission_gr_kWh'] = (df_data['totalCO2_gr'] / (df_data['totalEnergy_MWh']*1000))

        # Convert 'start' column to datetime and set timezone
        df_data['start'] = pd.to_datetime(df_data['start']).dt.tz_localize(None).dt.tz_localize('Europe/Berlin')
        df_data['lastUpdated'] = datetime.datetime.now()

        # Specify columns to upload
        columns_to_upload = ['start', 'unix_timestamp', 'totalEnergy_MWh', 'totalCO2_gr', 'emission_gr_kWh','lastUpdated']
        logging.debug('calculateCo2ForDay - Calculations finished, now deleting old rows')
        # Remove existing CO2 data for the day
        removeCo2DataForDay(day)

        # Upload data to the database
        logging.debug('calculateCo2ForDay - Uploading new data to ')
        df_data[columns_to_upload].to_sql(name=db_table_raw_data, con=engine, if_exists='append', index=False)

    except ValueError as e:
        print(e)
        # Log or handle the error appropriately

    finally:
        # Close cursor and perform any cleanup operations
        cursor.close()
        logging.debug('calculateCo2ForDay - Finished ')

if __name__ == "__main__":
    calculateCo2ForDay(datetime.date(2024,5,24))