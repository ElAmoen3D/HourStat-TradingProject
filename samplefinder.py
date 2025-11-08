import pandas as pd
from datetime import timedelta

all_results = pd.read_csv("NQ_HourStat_Results.csv")

all_examples = pd.DataFrame()
numSamples = 0

for index, row in all_results.iterrows():

    current_worked = row["Worked"]

    if current_worked:
        all_examples = pd.concat([all_examples, pd.DataFrame([row])], ignore_index=True)
        numSamples += 1

    if numSamples >= 25:
        break

numSamples = 0

for index, row in all_results.iterrows():

    current_worked = row["Worked"]

    if not current_worked:
        all_examples = pd.concat([all_examples, pd.DataFrame([row])], ignore_index=True)
        numSamples += 1

    if numSamples >= 25:
        break

all_examples.to_csv("Sampled_Patterns.csv", index=False)