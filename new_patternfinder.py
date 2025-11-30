## PATTERN FINDING - STOCK TRADING PROJECT - GABRIEL GONZALEZ ###
# This program should scan through the nq-1m_bk.csv file, find   #
# hour intervals where NQ Stats' Hour Stat is applicable, and    #
# determine whether it is a success or not. Should parse through #
#                       entire file                              #


# Import required libraries

import pandas as pd
from datetime import timedelta, datetime

# ===============================
# CONFIGURATION
# ===============================

DATA_FILE = "nq-1m_bk.csv"     # Input file path
OUTPUT_FILE = "NQ_HourStat_Results.csv" # Output results

# ===============================
# LOAD AND PREP DATA
# ===============================

print("Loading data...")
def readData(file_path):
    df = pd.read_csv(file_path, delimiter=";", names=["Date", "Time", "Open", "High", "Low", "Close", "Volume"], header=0)

    df = df.dropna()
    df = df.drop(columns=["Volume"])

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
    df = df.set_index("Datetime")

    df.to_csv("NQ_temp_processed.csv")

    return df


df = pd.read_csv("NQ_temp_processed.csv", parse_dates=["Datetime"], index_col="Datetime")
print(df.head())


# ===============================
# HELPER FUNCTION: Check pattern
# ===============================

def check_hour_stat(h1_start, h2_start, df):
    
    h1 = df.loc[h1_start:h1_start + timedelta(hours=1) - timedelta(minutes=1)]
    h2 = df.loc[h2_start:h2_start + timedelta(hours=1) - timedelta(minutes=1)]

    if h1.empty or h2.empty:
        print("One of the hours is empty, skipping...")
        print(f"h1_start: {h1_start}, h2_start: {h2_start}")
        return None
    
    h2_open = h2.loc[h2_start]["Open"]

    # if it's a series, get the value
    if isinstance(h2_open, pd.Series):
        h2_open = h2_open.iloc[0]


    # find hour 1 high and low
    h1_high = h1["High"].max()
    h1_low = h1["Low"].min()

    # find if hour 2 opens inside hour 1 range
    if not (h1_low <= h2_open <= h1_high):
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

    return_time = "N/A"
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

    
    # make sure all numbers in dictionary are standard python types
    
    result_dict = {"H1_Start": h1_start,
        "H2_Start": h2_start,
        "Direction": direction,
        "H1_High": h1_high,
        "H1_Low": h1_low,
        "H2_Open": h2_open,
        "Sweep_Time": sweep_time,
        "Return_Time": return_time,
        "Worked": bool(returned)
    }

    print(f"Found Hour Stat from {h1_start} to {h2_start}, Direction: {direction}, Worked: {returned}")
    return result_dict

# ===============================
# SCAN THROUGH HOUR PAIRS
# ===============================

print("Scanning hour pairs...")
res_df = pd.DataFrame()

# last 6 months entry in dataframe
current_time = df.index[0]  # approx last 6 months
end_time = df.index[-1]  

while current_time + timedelta(hours=2) <= end_time:
    # take only day and hour of current_time
    window_start = current_time.replace(minute=0, second=0, microsecond=0)
    window_end = window_start + timedelta(hours=2)


    #make sure both hours exist in dataframe
    if window_end - timedelta(minutes=1) not in df.index:
        current_time += timedelta(hours=1)
        continue
    
    #filter df to only include rows within this window
    window_df = df.loc[window_start:window_end - timedelta(minutes=1)]
    h1_start = window_start
    h2_start = None
    
    for row in window_df.itertuples():
        if row.Index >= h1_start + timedelta(hours=1):
            h2_start = row.Index
            break


    if (current_time <= current_time.replace(hour=16, minute=0)) or (current_time >= current_time.replace(hour=9, minute=30)):
        result = check_hour_stat(h1_start, h2_start, window_df)
    else:
       #skip to next hour
        current_time += timedelta(hours=1)
        continue

    if result:
        # convert result to DataFrame row
        result = pd.DataFrame([result])
        res_df = pd.concat([res_df, result], ignore_index=True)
    else:
        print(f"No valid Hour Stat found for hours starting at {h1_start} and {h2_start}.")

    current_time += timedelta(minutes=60)

# ===============================
# CREATE RESULT DATAFRAME
# ===============================

if not res_df.empty:
    win_rate = res_df["Worked"].mean() * 100
    print("\n=========== HOUR STAT RESULTS ===========")
    print(res_df)
    print(f"\nTotal setups found: {len(res_df)}")
    print(f"Win rate: {win_rate:.2f}%")
else:
    print("No valid Hour Stat setups found in this dataset.")

# Hourly hitrates

for hour in range(24):
    if 8 <= hour <= 15:
        hour_df = res_df[res_df["H2_Start"].dt.hour == hour]
        if not hour_df.empty:
            hour_win_rate = hour_df["Worked"].mean() * 100
            print(f"Hour {hour}: Setups: {len(hour_df)}, Win Rate: {hour_win_rate:.2f}%")
        else:
            print(f"Hour {hour}: No setups found.")

print("\nMonthly hitrates")

for month in range(12):

    month_df = res_df[res_df["H2_Start"].dt.month == month + 1]
    if not month_df.empty:
        month_win_rate = month_df["Worked"].mean() * 100
        print(f"Month {month + 1}: Setups: {len(month_df)}, Win Rate: {month_win_rate:.2f}%")
    else:
        print(f"Month {month + 1}: No setups found.")

    

# ===============================
# SAVE TO CSV
# ===============================

res_df.to_csv(OUTPUT_FILE, index=False)
print(f"\nResults saved to: {OUTPUT_FILE}")
