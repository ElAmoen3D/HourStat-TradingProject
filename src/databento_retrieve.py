import databento as db
import os
import dotenv as dt
import pandas as pd

dt.load_dotenv()  # Load environment variables from .env file
DATABENTO_APIKEY = os.getenv("DATABENTO_APIKEY")  # Get API key from environment variable
# 1. Create historical client with your API key
client = db.Historical(DATABENTO_APIKEY)

# 2. Request OHLCV-1m data for the continuous contract
data = client.timeseries.get_range(
    dataset="GLBX.MDP3",
    symbols="NQ.v.0",  # .v.0 rolls with the contract with higher volume
    schema="ohlcv-1m",
    stype_in="continuous",
    start="2025-08-29",
    end="2025-09-09",
)

# 3. Convert to DataFrame
df = data.to_df()
print(df)

# include tstamp in the csv file
df = df.reset_index()  # Reset index to include 'tstamp' as a column

df.to_csv("databento_data.csv", index=False)
