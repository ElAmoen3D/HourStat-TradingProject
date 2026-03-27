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

DATA_FILE = "nq-5m_bk.csv"     # Input file path
OUTPUT_FILE = "./results/NQ_HourStat_Results.csv" # Output results

# ===============================
# LOAD AND PREP DATA
# ===============================

print("Loading data...")
def readData(file_path : str):
    """
    Docstring for readData:
    Takes a path from a .csv file containing stock data and transforms it into a pandas dataframe usable to track hour stats
    
    :param file_path: Path of data csv file
    :type file_path: str
    """
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

    df = df.drop(columns=["Date", "Time"])

    # Set Datetime as index
    df = df.set_index("Datetime")

    df.to_csv("NQ_temp_processed.csv")

    return df

readData("nq-1m_bk.csv")
df = pd.read_csv("NQ_temp_processed.csv", parse_dates=["Datetime"], index_col="Datetime")
print(df.head())


# ===============================
# HELPER FUNCTION: Check pattern
# ===============================

def check_hour_stat(h1_start : datetime, 
                    h2_start: datetime, 
                    df: pd.DataFrame):
    
    """
    Docstring for check_hour_stat:
    
    Determines whether hour stat is true for two hour blocks, e.g, 8-9 and 9-10
    
    :param h1_start: Time at which the first hour begins
    :type h1_start: datetime

    :param h2_start: Time at which the second hour begins
    :type h2_start: datetime
    
    :param df: Dataframe to index with hour times
    :type df: pd.DataFrame
    """


    
    h1 = df.loc[h1_start:h1_start + timedelta(hours=1) - timedelta(minutes=1)]
    h2 = df.loc[h2_start:h2_start + timedelta(hours=1) - timedelta(minutes=1)]

    if h1.empty or h2.empty:
        print("One of the hours is empty, skipping...")
        print(f"h1_start: {h1_start}, h2_start: {h2_start}")
        return None

    
    h2_open = h2.iloc[0]["Open"]

    # find hour 1 high and low
    h1_high = h1["High"].max()
    h1_low = h1["Low"].min()

    # find if hour 2 opens inside hour 1 range
    if not (h1_low <= h2_open <= h1_high):
        return None
    
    # determine sweep within first twenty minutes of hour 2
    h2_first_20 = h2.loc[h2.index[0]:h2.index[0] + timedelta(minutes=19)]

    sweep_time = None
    direction = None
    
    # Check for sweep more efficiently
    up_sweep = (h2_first_20["High"].max() > h1_high)
    down_sweep = (h2_first_20["Low"].min() < h1_low)
    
    if down_sweep:
        direction = "Down"
        sweep_time = h2_first_20[h2_first_20["Low"] < h1_low].index[0]
    elif up_sweep:
        direction = "Up"
        sweep_time = h2_first_20[h2_first_20["High"] > h1_high].index[0]

    if sweep_time is None: return None

    # start searching for return from sweep time onward (same candle is OK)
    h2_from_sweep = h2.loc[sweep_time:]

    return_time = "N/A"
    returned = False

    if (direction == "Down"):
        for time, row in h2_from_sweep.iterrows():
            if row["High"] > h2_open:
                return_time = time
                returned = True
                break
    else:
        for time, row in h2_from_sweep.iterrows():
            if row["Low"] < h2_open:
                return_time = time
                returned = True
                break

    # make sure all numbers in dictionary are standard python types
    
    result_dict = {"H1_Start": h1_start,
        "H2_Start": h2_start,
        "Direction": direction,
        "H1_High": h1_high,
        "H1_Low": h1_low,
        "H2_Open": h2_open,
        "Sweep_Time": sweep_time,
        "Return_time": return_time,
        "Worked": bool(returned)
    }

    print(f"Found Hour Stat from {h1_start} to {h2_start}, Direction: {direction}, Worked: {returned}")
    return result_dict

# ===============================
# SCAN THROUGH HOUR PAIRS
# ===============================

print("Scanning hour pairs...")
all_df = pd.DataFrame()
pos_df = pd.DataFrame()
neg_df = pd.DataFrame()

# last 6 months entry in dataframe
current_time = df.index[0]  # approx last 6 months
end_time = df.index[-1] 

# dataframe with all results
all_df = pd.DataFrame()

while current_time + timedelta(hours=2) <= end_time:

    window_start = current_time 
    
    # Filter to 2-hour window instead of searching for exact end time
    window_df = df.loc[window_start:window_start + timedelta(hours=2)]

    # continue if window is empty
    if window_df.empty:
        current_time += timedelta(minutes=60)
        continue

    h1_start = window_df.index[0]
    h2_start = None
    
    # Find first index that's >= 1 hour from h1_start
    mask = window_df.index >= h1_start + timedelta(hours=1)
    if mask.any():
        h2_start = window_df.index[mask][0]


 
    if h2_start is None:
        current_time += timedelta(hours=1)
        continue


    result = check_hour_stat(h1_start, h2_start, window_df)

    if result:
        # convert result to DataFrame row
        if result.get("Direction") == "Down":
            all_df = pd.concat([all_df, pd.DataFrame([result])], ignore_index=True)
            result = pd.DataFrame([result])
            neg_df = pd.concat([neg_df, result], ignore_index=True)
        else:
            all_df = pd.concat([all_df, pd.DataFrame([result])], ignore_index=True)
            result = pd.DataFrame([result])
            pos_df = pd.concat([pos_df, result], ignore_index=True)
        
    else:
        print(f"No valid Hour Stat found for hours starting at {h1_start} and {h2_start}.")

    current_time += timedelta(minutes=60)

# ===============================
# CREATE RESULT DATAFRAME
# ===============================

if not pos_df.empty:
    pos_win_rate = pos_df["Worked"].mean() * 100
    print("\n=========== UP HOUR STAT RESULTS ===========")
    print(pos_df)
    print(f"\nTotal setups found: {len(pos_df)}")
    print(f"Win rate: {pos_win_rate:.2f}%")

    neg_win_rate = neg_df["Worked"].mean() * 100
    print("\n=========== DOWN HOUR STAT RESULTS ===========")
    print(neg_df)
    print(f"\nTotal setups found: {len(neg_df)}")
    print(f"Win rate: {neg_win_rate:.2f}%")
else:
    print("No valid Hour Stat setups found in this dataset.")

# Hourly hitrates

upset_df = pd.DataFrame(columns=["Hour", "Setups", "Hit Rate"])
downset_df = pd.DataFrame(columns=["Hour", "Setups", "Hit Rate"])
for hour in range(24):
    if 8 <= hour <= 15:
        pos_hour_df = pos_df[pos_df["H2_Start"].dt.hour == hour]
        neg_hour_df = neg_df[neg_df["H2_Start"].dt.hour == hour]
        if not pos_hour_df.empty:
            hour_pos_win_rate = pos_hour_df["Worked"].mean() * 100
            upset_df = pd.concat([upset_df, pd.DataFrame([{"Hour": hour, "Setups": len(pos_hour_df), "Hit Rate": hour_pos_win_rate}])], ignore_index=True)
        else:
            print(f"Hour {hour}: No setups found.")

        if not neg_hour_df.empty:
            hour_neg_win_rate = neg_hour_df["Worked"].mean() * 100
            downset_df = pd.concat([downset_df, pd.DataFrame([{"Hour": hour, "Setups": len(neg_hour_df), "Hit Rate": hour_neg_win_rate}])], ignore_index=True)
            print(f"Hour {hour}: Down Setups: {len(neg_hour_df)}, Win Rate: {hour_neg_win_rate:.2f}%")
        else:
            print(f"Hour {hour}: No setups found.")

print("\nMonthly hitrates")

for month in range(12):

    month_df = pos_df[pos_df["H2_Start"].dt.month == month + 1]
    if not month_df.empty:
        month_win_rate = month_df["Worked"].mean() * 100
        print(f"Month {month + 1}: Setups: {len(month_df)}, Win Rate: {month_win_rate:.2f}%")
    else:
        print(f"Month {month + 1}: No setups found.")

    

# ===============================
# SAVE TO CSV
# ===============================

# convert datetime columns to string for CSV
all_df["H1_Start"] = all_df["H1_Start"].astype(str)
all_df["H2_Start"] = all_df["H2_Start"].astype(str)
all_df["Sweep_Time"] = all_df["Sweep_Time"].astype(str)
all_df["Return_time"] = all_df["Return_time"].astype(str)
all_df.to_csv(OUTPUT_FILE, index=False)

downset_df.to_csv("./results/NQ_HourStat_Downset_HitRates.csv", index=False)
upset_df.to_csv("./results/NQ_HourStat_Upset_HitRates.csv", index=False)

print(f"\nResults saved to: {OUTPUT_FILE}")
