# script to find all holes in nq_temp_processed.csv
import pandas as pd
from datetime import timedelta, datetime

all_data = pd.read_csv("NQ_temp_processed.csv", parse_dates=["Datetime"], index_col="Datetime")

for row in all_data.itertuples():
    current_time = row.Index
    next_time = current_time + timedelta(minutes=1)
    # if next time not 4 pm
    if next_time not in all_data.index and next_time != current_time.replace(hour=16, minute=0):
        print(f"Missing data at: {next_time}")