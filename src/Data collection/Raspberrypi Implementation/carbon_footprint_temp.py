import requests
import pandas as pd
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
from pytz import timezone

psrTypeDict = {
    "A03": "Mixed",
    "A04": "Generation",
    "A05": "Load",
    "B01": "Biomass",
    "B02": "Fossil Brown coal/Lignite",
    "B03": "Fossil Coal-derived gas",
    "B04": "Fossil Gas",
    "B05": "Fossil Hard coal",
    "B06": "Fossil Oil",
    "B07": "Fossil Oil shale",
    "B08": "Fossil Peat",
    "B09": "Geothermal",
    "B10": "Hydro Pumped Storage",
    "B11": "Hydro Run-of-river and poundage",
    "B12": "Hydro Water Reservoir",
    "B13": "Marine",
    "B14": "Nuclear",
    "B15": "Other renewable",
    "B16": "Solar",
    "B17": "Waste",
    "B18": "Wind Offshore",
    "B19": "Wind Onshore",
    "B20": "Other",
    "B21": "AC Link",
    "B22": "DC Link",
    "B23": "Substation",
    "B24": "Transformer"
}

# Replace with your API key
api_key = 'abc0aade-4dcc-42cf-a645-b49943e6ea97'

# Define the parameters
document_type = 'A75'  # Document type for Actual Generation per Production Type
country_code = '10Y1001A1001A83F'  # Bidding zone domain code for Germany
date_format = '%Y%m%d%H%M'
start_date = 202405230000
end_date = 202405232345

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
print(url)



# Make the GET request
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
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
    print(df)
    # Convert 'Start' and 'End' columns to datetime
    df['Start'] = pd.to_datetime(df['Start'])
    df['End'] = pd.to_datetime(df['End'])
    df["Position"] = pd.to_numeric(df['Position'])

    df['psrTypeText'] = df['psrType'].map(psrTypeDict)

    df['End'] = df['Start'] + pd.to_timedelta(df['Position'] * 15, unit='m')
    df['Start'] = df['Start'] + pd.to_timedelta((df['Position']-1) * 15, unit='m')

    local_timezone = timezone('Europe/Berlin')  # Replace 'Your_Local_Timezone' with your local timezone
    df['Start'] = df['Start'].dt.tz_localize(None).dt.tz_localize(local_timezone)
    df['End'] = df['End'].dt.tz_localize(None).dt.tz_localize(local_timezone)

    # df['Start'] = df.apply(lambda row: row['Start'] + pd.Timedelta(minutes=(row['Position']-1) * 15), axis=1)
    print(df)
    pivot_df = df.pivot(index='Start', columns='psrTypeText', values='Quantity')
    print(pivot_df)
else:
    print(f"Error: {response.status_code}")
    print(response.text)