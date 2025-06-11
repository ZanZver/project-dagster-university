# Local imports
import requests
from dagster_essentials.assets import constants
# Dagster import
import dagster as dg

@dg.asset # Declare an asset 
def taxi_trips_file() -> None:
    month_to_fetch = "2023-03"
    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )
    
    with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb") as output_file:
        output_file.write(raw_trips.content)