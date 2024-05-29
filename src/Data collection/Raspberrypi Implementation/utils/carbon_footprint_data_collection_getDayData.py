import requests
import pandas as pd
from datetime import datetime,timedelta
import xml.etree.ElementTree as ET
from pytz import timezone
from definitions import energyTypeDict
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Replace with your API key
api_key = 'abc0aade-4dcc-42cf-a645-b49943e6ea97'

# Define the parameters
document_type = 'A75'  # Document type for Actual Generation per Production Type
country_code = '10Y1001A1001A83F'  # Bidding zone domain code for Germany


def get_EnergyActualGenerationOfDay(day) -> pd.DataFrame:
    logging.debug('get_EnergyActualGenerationOfDay - Start')
    
    # Expected day format: YYYYMMDD in datetime.date
    formatted_date = day.strftime('%Y%m%d')
    formatted_next_day = (day + timedelta(days=1)).strftime('%Y%m%d')
    start_date = formatted_date+"0000"
    end_date = formatted_next_day +"0000"
    # Construct the URL
    url = (
        f'https://web-api.tp.entsoe.eu/api'
        f'?securityToken={api_key}'
        f'&documentType={document_type}'
        f'&processType=A16'  # Process type for actual generation
        f'&in_Domain={country_code}'
        f'&periodStart={start_date}'
        f'&periodEnd={end_date}'
    )

    # Make the GET request
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        logging.debug('get_EnergyActualGenerationOfDay - Got response from entsoe service')
        root = ET.fromstring(response.content)
        namespaces = {'ns': 'urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0'}
        # Extract relevant data
        data = []
        for series in root.findall('.//ns:TimeSeries', namespaces):
            domain_mRID = series.find('.//ns:inBiddingZone_Domain.mRID[@codingScheme="A01"]', namespaces)
            if domain_mRID is not None and domain_mRID.text == '10Y1001A1001A83F':
                psr_type = series.find('.//ns:psrType', namespaces).text
                start_time = series.find('.//ns:timeInterval/ns:start', namespaces).text
                end_time = series.find('.//ns:timeInterval/ns:end', namespaces).text
                for point in series.findall('.//ns:Point', namespaces):
                    position = point.find('ns:position', namespaces).text
                    quantity = point.find('ns:quantity', namespaces).text
                    data.append({'Position': position, 'Quantity': quantity, 'psrType': psr_type, 'Start': start_time, 'End': end_time})

        # Create DataFrame
        df = pd.DataFrame(data)
        # print(df)
        # Convert 'Start' and 'End' and Position columns to datetime and int
        df[['Start', 'End']] = df[['Start', 'End']].apply(pd.to_datetime)
        df["Position"] = pd.to_numeric(df['Position'])
        # Get the name out of the psrType column using the energyTypeDict
        df['psrTypeText'] = df['psrType'].map(energyTypeDict)

        # Setting the exact time of each measurement
        df['End'] = df['Start'] + pd.to_timedelta(df['Position'] * 15, unit='m')
        df['Start'] = df['Start'] + pd.to_timedelta((df['Position']-1) * 15, unit='m')

        # Setting the timezone correctly
        local_timezone = timezone('Europe/Berlin') 
        df['Start'] = df['Start'].dt.tz_localize(None).dt.tz_localize(local_timezone)
        df['End'] = df['End'].dt.tz_localize(None).dt.tz_localize(local_timezone)
        

        df = df.pivot(index='Start', columns='psrTypeText', values='Quantity')
        df.reset_index(inplace=True)
        df['unix_timestamp'] = df['Start'].astype('int64') // 10**9
        logging.debug('get_EnergyActualGenerationOfDay - Dataframe created and sent')
        return df


    else:
        raise ValueError(f"Error: {response.status_code} in: {response.text}")
    
# date = datetime.date(2024,5,23)
# get_EnergyActualGenerationOfDay(date)
