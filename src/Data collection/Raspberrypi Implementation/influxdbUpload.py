from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import csv

# Define InfluxDB connection parameters
url = "http://10.42.0.250:8086"
token = "-_cyKUdgmlAFa3oQw8QASF7e85qJbMCWyQcXK338rK24awex3Jyvl9ZsxNDeG4KlyhhVdNoBelain742BNBGZg=="
org = "AVT"
bucket = "carbon_footprint_data"

# Initialize InfluxDB client
client = InfluxDBClient(url=url, token=token)

# Initialize write API
write_api = client.write_api(write_options=SYNCHRONOUS)

# CSV file path
csv_file = "/home/yannick/Desktop/carbon_footprint_data.csv"

# Read CSV file and write data to InfluxDB
with open(csv_file, 'r') as file:
    reader = csv.DictReader(file, delimiter=';')
    for row in reader:
        print(row)
        point = Point("carbon_footprint_data") \
            .field("Biomass", str(row["Biomass"])) \
            .field("Fossil Brown coal/Lignite", str(row["Fossil Brown coal/Lignite"])) \
            .field("Fossil Coal-derived gas", str(row["Fossil Coal-derived gas"])) \
            .field("Fossil Gas", str(row["Fossil Gas"])) \
            .field("Fossil Hard coal", str(row["Fossil Hard coal"])) \
            .field("Fossil Oil", str(row["Fossil Oil"])) \
            .field("Fossil Oil shale", str(row["Fossil Oil shale"])) \
            .field("Fossil Peat", str(row["Fossil Peat"])) \
            .field("Geothermal", str(row["Geothermal"])) \
            .field("Hydro Pumped Storage", str(row["Hydro Pumped Storage"])) \
            .field("Hydro Pumped Storage.1", str(row["Hydro Pumped Storage.1"])) \
            .field("Hydro Run-of-river and poundage", str(row["Hydro Run-of-river and poundage"])) \
            .field("Hydro Water Reservoir", str(row["Hydro Water Reservoir"])) \
            .field("Marine", str(row["Marine"])) \
            .field("Nuclear", str(row["Nuclear"])) \
            .field("Other", str(row["Other"])) \
            .field("Other renewable", str(row["Other renewable"])) \
            .field("Solar", str(row["Solar"])) \
            .field("Waste", str(row["Waste"])) \
            .field("Wind Offshore", str(row["Wind Offshore"])) \
            .field("Wind Onshore", str(row["Wind Onshore"])) \
            .field("start", str(row["start"])) \
            .field("end", str(row["end"])) \
            .field("start_datetime", row["start_datetime"]) \
            .field("start_unix_timestamp", str(row["start_unix_timestamp"])) \
            .field("end_datetime", row["end_datetime"]) \
            .field("end_unix_timestamp", str(row["end_unix_timestamp"])) \
            .field("Biomass_co2", str(row["Biomass_co2"])) \
            .field("Fossil Brown coal/Lignite_co2", str(row["Fossil Brown coal/Lignite_co2"])) \
            .field("Fossil Coal-derived gas_co2", str(row["Fossil Coal-derived gas_co2"])) \
            .field("Fossil Gas_co2", str(row["Fossil Gas_co2"])) \
            .field("Fossil Hard coal_co2", str(row["Fossil Hard coal_co2"])) \
            .field("Fossil Oil_co2", str(row["Fossil Oil_co2"])) \
            .field("Fossil Oil shale_co2", str(row["Fossil Oil shale_co2"])) \
            .field("Fossil Peat_co2", str(row["Fossil Peat_co2"])) \
            .field("Geothermal_co2", str(row["Geothermal_co2"])) \
            .field("Hydro Pumped Storage_co2", str(row["Hydro Pumped Storage_co2"])) \
            .field("Hydro Pumped Storage.1_co2", str(row["Hydro Pumped Storage.1_co2"])) \
            .field("Hydro Run-of-river and poundage_co2", str(row["Hydro Run-of-river and poundage_co2"])) \
            .field("Hydro Water Reservoir_co2", str(row["Hydro Water Reservoir_co2"])) \
            .field("Marine_co2", str(row["Marine_co2"])) \
            .field("Nuclear_co2", str(row["Nuclear_co2"])) \
            .field("Other_co2", str(row["Other_co2"])) \
            .field("Other renewable_co2", str(row["Other renewable_co2"])) \
            .field("Solar_co2", str(row["Solar_co2"])) \
            .field("Waste_co2", str(row["Waste_co2"])) \
            .field("Wind Offshore_co2", str(row["Wind Offshore_co2"])) \
            .field("Wind Onshore_co2", str(row["Wind Onshore_co2"])) \
            .field("totalEnergy_MWh", str(row["totalEnergy_MWh"])) \
            .field("totalCO2_gr", str(row["totalCO2_gr"])) \
            .field("emission_gr_kWh", str(row["emission_gr_kWh"])) \
            .time(row["start_datetime"], WritePrecision.S)  # Assuming "start_datetime" is the timestamp
        write_api.write(bucket=bucket, org=org, record=point)

print("Data successfully written to InfluxDB.")