import pandas as pd


def format_df(file, col_name):
    df = pd.read_csv(file,
           names=['station_id', 'month', 2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32],
           delim_whitespace=True,
           header=None,
           )
    df[col_name] = df[[2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32]].values.tolist()
    df.drop([2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32], axis=1, inplace=True)
    return df

def daily_stations_df():
    max_temps = format_df('../data/dly-tmax-normal.csv', 'max_temps')
    min_temps = format_df('../data/dly-tmin-normal.csv', 'min_temps')
    gdd_40 = format_df('../data/dly-grdd-base40.csv', 'gdd_40')
    gdd_50 = format_df('../data/dly-grdd-base50.csv', 'gdd_50')
    mtd_precip = format_df('../data/mtd-prcp-normal.csv', 'mtd_precip')
    mtd_snow = format_df('../data/mtd-snow-normal.csv', 'mtd_snow')
    ytd_precip = format_df('../data/ytd-prcp-normal.csv', 'ytd_precip')
    ytd_snow = format_df('../data/ytd-snow-normal.csv', 'ytd_snow')
    dly_precip_50 = format_df('../data/dly-prcp-50pctl.csv', 'dly_precip_50')
    dly_precip_75 = format_df('../data/dly-prcp-75pctl.csv', 'dly_precip_75')
    print(mtd_snow[100:120])

daily_stations_df()
