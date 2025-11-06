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

# dictionary with every hour of the trading day
trading_hours = [f"{hour:02d}:00" for hour in range(9, 17)]
hours_dict = {}

for hour in trading_hours:
    hours_dict[hour] = pd.DataFrame(columns=["Date", "Time", "Worked"])


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



# ===============================
# HELPER FUNCTION: Check pattern
# ===============================

def check_hour_stat(h1_start, h2_start, df):
    
    h1 = df.loc[h1_start:h1_start + timedelta(hours=1) - timedelta(minutes=1)]
    h2 = df.loc[h2_start:h2_start + timedelta(hours=1) - timedelta(minutes=1)]

    try:
        h2_open = h2["Open"].iloc[0]
    except Exception as e:
        return None

    # handle error cases
    if h1.empty or h2.empty:
        return None
    
    # find hour 1 high and low
    h1_high = h1["High"].max()
    h1_low = h1["Low"].min()

    # find if hour 2 opens inside hour 1 range
    if not (h1_low <= h2["Open"].iloc[0] <= h1_high):
        return None
    
    # determine sweep within first twenty minutes of hour 2
    h2_first_20 = h2.loc[h2.index[0]:h2.index[0] + timedelta(minutes=19)]
    h2_last_40 = h2.loc[h2.index[0] + timedelta(minutes=20):h2.index[0] + timedelta(minutes=59)]

    sweep_time = None

    for row in h2_first_20.itertuples():
        if row.High > h1_high:
            direction = "Down"
            sweep_time = row.Index
            break
        elif row.Low < h1_low:
            direction = "Up"
            sweep_time = row.Index
            break

    if sweep_time is None: return None

    return_time = None
    returned = False

    # determine return within last forty minutes of hour 2
    for row in h2_last_40.itertuples():
        if direction == "Down" and row.Low < h2_open:
            returned = True
            return_time = row.Index
            break
        elif direction == "Up" and row.High > h2_open:
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

# first entry in dataframe
current_time = df.index[0]
end_time = df.index[-1]  

while current_time + timedelta(hours=2) <= end_time:
    h1_start = current_time
    h2_start = current_time + timedelta(hours=1)

    result = check_hour_stat(h1_start, h2_start, df)
    if result:
        results.append(result)

    current_time += timedelta(minutes=60)

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
