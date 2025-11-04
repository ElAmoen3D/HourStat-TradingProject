### PATTERN FINDING - STOCK TRADING PROJECT - GABRIEL GONZALEZ ###
# This program should scan through the nq-1m_bk.csv file, find   #
# hour intervals where NQ Stats' Hour Stat is applicable, and    #
# determine whether it is a success or not. Should parse through #
#                       entire file                              #


# Import required libraries

import pandas as pd
from datetime import timedelta

# ===============================
# CONFIGURATION
# ===============================

DATA_FILE = "nq-1m_bk.csv"     # Input file path
OUTPUT_FILE = "NQ_HourStat_Results.csv" # Output results

# ===============================
# LOAD AND PREP DATA
# ===============================

print("Loading data...")
df = pd.read_csv(DATA_FILE, delimiter=";", names=["Date", "Time", "Open", "High", "Low", "Close", "Volume"], header=0)

# change time into timedelta
df["Time"] = pd.to_timedelta(df["Time"].astype(str) + ":00")

print("Time changed to timedelta...")
# Change date to timestamp
df["Date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y")
print("Date changed to datetime...")

# Combine date and time into single datetime column
df["Datetime"] = df["Date"] + df["Time"]
print("Datetime column created...")

# Set Datetime as index
df = df.sort_values("Datetime").set_index("Datetime")

# ===============================
# RESAMPLE TO HOURLY CANDLES
# ===============================

print("Resampling to hourly candles...")
hourly = df.resample("1H").agg({
    "Open": "first",
    "High": "max",
    "Low": "min",
    "Close": "last"
})

# ===============================
# HELPER FUNCTION: Check pattern
# ===============================

def check_hour_stat(h1_start, h2_start, df, hourly):
    h1 = hourly.loc[h1_start]
    h2 = hourly.loc[h2_start]

    # condition 1: H2 opens inside H1 range
    if not (h1.Low < h2.Open < h1.High):
        return None

    # define time windows
    h2_first_20m = df.loc[h2_start:h2_start + timedelta(minutes=19)]
    h2_full = df.loc[h2_start:h2_start + timedelta(minutes=59)]

    h2_last_40m = df.loc[h2_start + timedelta(minutes=20):h2_start + timedelta(minutes=59)]

    direction = None
    sweep_time = None

    # condition 2: Sweep within first 20 minutes
    if (h2_first_20m["High"] > h1.High).any():
        direction = "High Sweep"
        sweep_time = h2_first_20m[h2_first_20m["High"] > h1.High].index[0]
    elif (h2_first_20m["Low"] < h1.Low).any():
        direction = "Low Sweep"
        sweep_time = h2_first_20m[h2_first_20m["Low"] < h1.Low].index[0]

    if direction is None:
        return None

    # condition 3: Return to open before close
    # iterate through full hour after sweep
 #   returned = False
  #  for timestamp, row in h2_full.iterrows():
   #     if direction == "High Sweep" and row["Low"] <= h2.Open:
    #        returned = True
     #       break
      #  elif direction == "Low Sweep" and row["High"] >= h2.Open:
       #     returned = True
  #          break
  #  return_time = None
  #  
  #  if returned:
  #      cross = h2_full[(h2_full["High"] >= h2.Open) & (h2_full["Low"] <= h2.Open)]
  #      if not cross.empty:
  #          return_time = cross.index[0]


    returned = False
    return_time = None
    for row in h2_last_40m.itertuples():
        if direction == "High Sweep" and row.Low <= h2.Open:
            returned = True
            return_time = row.Index
            break
        elif direction == "Low Sweep" and row.High >= h2.Open:
            returned = True
            return_time = row.Index
            break

    return {
        "H1_Start": h1_start,
        "H2_Start": h2_start,
        "Direction": direction,
        "H1_High": h1.High,
        "H1_Low": h1.Low,
        "H2_Open": h2.Open,
        "Sweep_Time": sweep_time,
        "Return_Time": return_time,
        "Worked": bool(returned)
    }

# ===============================
# SCAN THROUGH HOUR PAIRS
# ===============================

print("Scanning hour pairs...")
results = []

for i in range(len(hourly) - 1):
    h1_start = hourly.index[i]
    h2_start = hourly.index[i + 1]
    res = check_hour_stat(h1_start, h2_start, df, hourly)
    if res:
        results.append(res)

# ===============================
# CREATE RESULT DATAFRAME
# ===============================

res_df = pd.DataFrame(results)
if not res_df.empty:
    win_rate = res_df["Worked"].mean() * 100
    print("\n=========== HOUR STAT RESULTS ===========")
    print(res_df.head())
    print(f"\nTotal setups found: {len(res_df)}")
    print(f"Win rate: {win_rate:.2f}%")
else:
    print("No valid Hour Stat setups found in this dataset.")

# ===============================
# SAVE TO CSV
# ===============================

res_df.to_csv(OUTPUT_FILE, index=False)
print(f"\nResults saved to: {OUTPUT_FILE}")
