import datetime
from time import sleep
from carbon_footprint_data_collection_getDayData import get_EnergyActualGenerationOfDay
from carbon_footprint_data_collection_insertAndRemoveData import insertDataIntoDatabase
from carbon_footprint_data_collection_calculateData import calculateCo2ForDay

# today = datetime.date(2024,5,21)
today = datetime.date.today()

df = get_EnergyActualGenerationOfDay(today)
sleep(1)
insertDataIntoDatabase(today,df)
print("sleeping for 1")
sleep(1)
calculateCo2ForDay(today)
