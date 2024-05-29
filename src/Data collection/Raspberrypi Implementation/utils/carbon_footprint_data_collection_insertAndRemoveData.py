from sqlalchemy import create_engine,text
import datetime
import pandas as pd
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Define InfluxDB connection parameters
url = "http://10.42.0.250:8086"
# Database connection parameters
db_username = 'user'
db_password = 'password'
db_hostname = "141.52.65.99:3306"
db_name = 'datenbank'
db_table_raw_data='carbon_footprint_rawData' 

# Initialize MySQL client
engine = create_engine(f'mysql+mysqlconnector://{db_username}:{db_password}@{db_hostname}/{db_name}')

def removeDataFromDatabase(day: datetime.date):
    logging.debug('removeDataFromDatabase - Start')
    # Ensure day is a valid date object
    if not isinstance(day, datetime.date):
        raise ValueError("Input 'day' must be a datetime.date object.")

    try:
        # Create start and end timestamps
        start_of_day_timestamp = int(datetime.datetime(day.year, day.month, day.day).timestamp())
        end_of_day_timestamp = int(datetime.datetime(day.year, day.month, day.day, 23, 59, 59).timestamp())

        # Construct SQL command without single quotes around timestamps
        sql_command = f"DELETE FROM {db_table_raw_data} WHERE unix_timestamp BETWEEN {start_of_day_timestamp} AND {end_of_day_timestamp};"
        logging.debug('removeDataFromDatabase - Sending SQL Command')
        # Execute SQL command
        connection = engine.connect()
        trans = connection.begin()
        try:
            connection.execute(text(sql_command))
            trans.commit()
            logging.debug('removeDataFromDatabase - Succcess - END')
        except Exception:
            trans.rollback()
            raise

        
        # Check the number of rows affected
        # print(f"Number of rows deleted: {result.rowcount}")

    except Exception as e:
        logging.error(f'removeDataFromDatabase - error {e}')
        # print(f"An error occurred: {e}")
    

def insertDataIntoDatabase(day: datetime.date, data_df: pd.DataFrame):
    logging.debug("insertDataIntoDatabase - Start")
    try:
        removeDataFromDatabase(day)
        logging.debug('insertDataIntoDatabase - Sending data to databse')
        data_df.to_sql(db_table_raw_data, con=engine, if_exists='append', index=False)
    except:
        logging.error('insertDataIntoDatabase - error')


if __name__ == "__main__":
    removeDataFromDatabase(datetime.date(2024,5,24))