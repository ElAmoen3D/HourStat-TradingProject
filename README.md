# HOURSTAT PATTERN FINDER
The script should iterate through a CSV of NQ prices of any date, find places in which a previous hour's low
is swept within the first 20 minutes, and determine if the price returns.

## Sample of successful hour stat
![samplepic](./screenshots/success1_tradesc.png)

This is an example of a succesful instance of the Hour Stat. After the previous hour's high is swept around the 30-minute mark, with the
price dropping and quickly returning to the hour's open.

## Goal - Find how often the strategy works
Determine a hitrate for the NQ prices from 2014 to 2024. First, determine if a two-hour window contains a sweep, then determine
if the price returns. Only sweeps are considered valid setups.

# Results - Overall Accuracy - 60%
Roughly, the hour stat held true for 60% of valid setups. Data sample:

H1_Start,H2_Start,Direction,H1_High,H1_Low,H2_Open,Sweep_Time,Return_Time,Worked
2014-01-02 09:00:00,2014-01-02 10:00:00,Down,4097.358072,4082.704126,4093.048088,2014-01-02 10:12:00,2014-01-02 10:38:00,True
2014-01-02 11:00:00,2014-01-02 12:00:00,Down,4092.473423,4084.715452,4092.186091,2014-01-02 12:00:00,2014-01-02 12:20:00,True
2014-01-02 13:00:00,2014-01-02 14:00:00,Down,4091.898759,4078.968807,4085.002784,2014-01-02 14:18:00,N/A,False
2014-01-03 01:00:00,2014-01-03 02:00:00,Down,4082.129462,4076.957481,4082.129462,2014-01-03 02:00:00,2014-01-03 02:20:00,True
2014-01-03 02:00:00,2014-01-03 03:00:00,Down,4084.42812,4076.957481,4083.566123,2014-01-03 03:02:00,N/A,False
2014-01-03 03:00:00,2014-01-03 04:00:00,Down,4090.174765,4083.566123,4089.887433,2014-01-03 04:00:00,N/A,False
2014-01-03 04:00:00,2014-01-03 05:00:00,Down,4092.473423,4088.738104,4091.324094,2014-01-03 05:06:00,2014-01-03 05:26:00,True
2014-01-03 06:00:00,2014-01-03 07:00:00,Down,4096.496075,4090.74943,4095.92141,2014-01-03 07:05:00,2014-01-03 07:29:00,True
2014-01-03 12:00:00,2014-01-03 13:00:00,Down,4065.751523,4060.579542,4064.602194,2014-01-03 13:02:00,2014-01-03 13:20:00,True
2014-01-06 00:01:00,2014-01-06 01:01:00,Down,4060.579542,4057.131555,4059.717545,2014-01-06 01:02:00,N/A,False
2014-01-06 06:00:00,2014-01-06 07:00:00,Down,4064.602194,4058.568216,4062.016203,2014-01-06 07:04:00,2014-01-06 07:20:00,True
2014-01-06 12:00:00,2014-01-06 13:00:00,Down,4049.660916,4034.144974,4047.64959,2014-01-06 13:12:00,N/A,False
2014-01-07 02:00:00,2014-01-07 03:00:00,Down,4053.108903,4046.212929,4053.108903,2014-01-07 03:00:00,2014-01-07 03:35:00,True
2014-01-07 03:00:00,2014-01-07 04:00:00,Down,4055.120229,4052.246906,4054.832897,2014-01-07 04:01:00,N/A,False
2014-01-07 08:00:00,2014-01-07 09:00:00,Down,4075.233487,4056.269558,4074.371491,2014-01-07 09:00:00,N/A,False

## Hourly Hit-rates:

Total setups found: 20095
Win rate: 60.56%
- Hour 8: Up Setups: 808, Win Rate: 83.29%
- Hour 8: Down Setups: 784, Win Rate: 86.48%
- Hour 9: Up Setups: 1000, Win Rate: 59.80%
- Hour 9: Down Setups: 1136, Win Rate: 60.30%
- Hour 10: Up Setups: 744, Win Rate: 61.16%
- Hour 10: Down Setups: 956, Win Rate: 57.43%
- Hour 11: Up Setups: 756, Win Rate: 62.04%
- Hour 11: Down Setups: 916, Win Rate: 61.14%
- Hour 12: Up Setups: 793, Win Rate: 64.06%
- Hour 12: Down Setups: 971, Win Rate: 62.82%
- Hour 13: Up Setups: 893, Win Rate: 63.27%
- Hour 13: Down Setups: 1002, Win Rate: 62.48%
- Hour 14: Up Setups: 836, Win Rate: 69.50%
- Hour 14: Down Setups: 981, Win Rate: 69.72%
- Hour 15: Up Setups: 545, Win Rate: 48.26%
- Hour 15: Down Setups: 697, Win Rate: 40.17%

# IMPORTANT - INPUT CSV IS NOT INCLUDED IN THIS REPOSITORY!

Add the input csv as 'nq-1m_bk.csv'. Input file must be in one-minute intervals

# Running:
- Ensure nq-1m_bk.csv is in the same directory as hour_stat.py
- Results csv will be in results directory