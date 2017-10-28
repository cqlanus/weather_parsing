import pandas as pd
import functools

# TODO: figure out if array return hundredths or tenths of inches; apply appropriate functions
def format_df(file, col_name, cb):
    df = pd.read_csv(file,
           names=['station_id', 'month', 2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32],
           delim_whitespace=True,
           header=None,
           )
    df[col_name] = df[[2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32]].values.tolist()
    df[col_name] = df[col_name].apply(cb)
    df.drop([2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32], axis=1, inplace=True)
    return df

def hundredths(str):
    return int(str[:-1])/100

def tenths(str):
    return int(str[:-1])/10

def to_int(str):
    return int(str[:-1])

def map_to_hundredths(arr):
    return list(map(hundredths, arr))

def map_to_tenths(arr):
    return list(map(tenths, arr))

def map_to_int(arr):
    return list(map(to_int, arr))

def identity(arr):
    return arr
def daily_stations_df():
    max_temps = format_df('../data/dly-tmax-normal.csv', 'max_temps', map_to_tenths)
    # print(max_temps[100:110])
    min_temps = format_df('../data/dly-tmin-normal.csv', 'min_temps', map_to_tenths)
    gdd_40 = format_df('../data/dly-grdd-base40.csv', 'daily_gdd_40', map_to_int)
    gdd_50 = format_df('../data/dly-grdd-base50.csv', 'daily_gdd_50', map_to_int)
    mtd_precip = format_df('../data/mtd-prcp-normal.csv', 'mtd_precip', map_to_hundredths)
    mtd_snow = format_df('../data/mtd-snow-normal.csv', 'mtd_snow', map_to_tenths)
    ytd_precip = format_df('../data/ytd-prcp-normal.csv', 'ytd_precip', map_to_hundredths)
    ytd_snow = format_df('../data/ytd-snow-normal.csv', 'ytd_snow', map_to_tenths)
    dly_precip_50 = format_df('../data/dly-prcp-50pctl.csv', 'daily_precip_50', map_to_hundredths)
    dly_precip_75 = format_df('../data/dly-prcp-75pctl.csv', 'daily_precip_75', map_to_hundredths)

    merge_criteria = ['station_id', 'month']
    temp_merge = pd.merge(max_temps, min_temps, on=merge_criteria)
    gdd_merge = pd.merge(gdd_40, gdd_50, on=merge_criteria)
    mtd_merge = pd.merge(mtd_precip, mtd_snow, on=merge_criteria)
    ytd_merge = pd.merge(ytd_precip, ytd_snow, on=merge_criteria)
    dly_precip_merge = pd.merge(dly_precip_50, dly_precip_75, on=merge_criteria)

    to_merge = [temp_merge, gdd_merge, mtd_merge, ytd_merge, dly_precip_merge]
    merged = functools.reduce(lambda left,right: pd.merge(left,right,on=merge_criteria,how='outer'), to_merge)

    merged.loc[merged['max_temps'].isnull(), ['max_temps']] = merged.loc[merged['max_temps'].isnull(), 'max_temps'].apply(lambda x: [])
    merged.loc[merged['min_temps'].isnull(), ['min_temps']] = merged.loc[merged['min_temps'].isnull(), 'min_temps'].apply(lambda x: [])
    merged.loc[merged['daily_gdd_40'].isnull(), ['daily_gdd_40']] = merged.loc[merged['daily_gdd_40'].isnull(), 'daily_gdd_40'].apply(lambda x: [])
    merged.loc[merged['daily_gdd_50'].isnull(), ['daily_gdd_50']] = merged.loc[merged['daily_gdd_50'].isnull(), 'daily_gdd_50'].apply(lambda x: [])
    merged.loc[merged['mtd_precip'].isnull(), ['mtd_precip']] = merged.loc[merged['mtd_precip'].isnull(), 'mtd_precip'].apply(lambda x: [])
    merged.loc[merged['mtd_snow'].isnull(), ['mtd_snow']] = merged.loc[merged['mtd_snow'].isnull(), 'mtd_snow'].apply(lambda x: [])
    merged.loc[merged['ytd_precip'].isnull(), ['ytd_precip']] = merged.loc[merged['ytd_precip'].isnull(), 'ytd_precip'].apply(lambda x: [])
    merged.loc[merged['ytd_snow'].isnull(), ['ytd_snow']] = merged.loc[merged['ytd_snow'].isnull(), 'ytd_snow'].apply(lambda x: [])
    merged.loc[merged['daily_precip_50'].isnull(), ['daily_precip_50']] = merged.loc[merged['daily_precip_50'].isnull(), 'daily_precip_50'].apply(lambda x: [])
    merged.loc[merged['daily_precip_75'].isnull(), ['daily_precip_75']] = merged.loc[merged['daily_precip_75'].isnull(), 'daily_precip_75'].apply(lambda x: [])
    return merged
    # print(merged[100:110])

# daily_stations_df()
