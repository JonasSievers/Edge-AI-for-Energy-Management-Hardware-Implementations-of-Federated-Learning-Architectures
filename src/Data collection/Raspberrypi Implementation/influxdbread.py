from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import csv

# Define InfluxDB connection parameters
url = "http://10.42.0.250:8086"
token = "-_cyKUdgmlAFa3oQw8QASF7e85qJbMCWyQcXK338rK24awex3Jyvl9ZsxNDeG4KlyhhVdNoBelain742BNBGZg=="
org = "AVT"
bucket = "carbon_footprint_data"


# Initialize the InfluxDB client
client = InfluxDBClient(url=url, token=token, org=org)

# Instantiate a query API client
query_api = client.query_api()

# Define Flux query to select all data from the bucket
query = f'from(bucket: "{bucket}") |> range(start: -100000h)'

# Execute the query
result = query_api.query(query)

# Print the results
for table in result:
    for row in table.records:
        print(row.values)
