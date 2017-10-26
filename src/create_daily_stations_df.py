import pandas as pd


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

def convert_to_float(str):
    if len(str) == 3:
        return float(str[0:2] + '.' + str[2])
    elif len(str) == 4:
        return float(str[0:3] + '.' + str[3])

def remove_last_char(str):
    return str[:-1]

def str_to_float(arr):
    return map(lambda x: convert_to_float(remove_last_char(x)), arr)

def daily_stations_df():
    max_temps = format_df('../data/dly-tmax-normal.csv', 'max_temps', str_to_float)
    print(max_temps[100:110])
    # min_temps = format_df('../data/dly-tmin-normal.csv', 'min_temps', remove_last_char)
    # gdd_40 = format_df('../data/dly-grdd-base40.csv', 'gdd_40', remove_last_char)
    # gdd_50 = format_df('../data/dly-grdd-base50.csv', 'gdd_50', remove_last_char)
    # mtd_precip = format_df('../data/mtd-prcp-normal.csv', 'mtd_precip', remove_last_char)
    # mtd_snow = format_df('../data/mtd-snow-normal.csv', 'mtd_snow', remove_last_char)
    # ytd_precip = format_df('../data/ytd-prcp-normal.csv', 'ytd_precip', remove_last_char)
    # ytd_snow = format_df('../data/ytd-snow-normal.csv', 'ytd_snow', remove_last_char)
    # dly_precip_50 = format_df('../data/dly-prcp-50pctl.csv', 'dly_precip_50', remove_last_char)
    # dly_precip_75 = format_df('../data/dly-prcp-75pctl.csv', 'dly_precip_75', remove_last_char)

    # merge_criteria = ['station_id', 'month']
    # temp_merge = pd.merge(max_temps, min_temps, on=merge_criteria)
    # gdd_merge = pd.merge(gdd_40, gdd_50, on=merge_criteria)
    # mtd_merge = pd.merge(mtd_precip, mtd_snow, on=merge_criteria)
    # ytd_merge = pd.merge(ytd_precip, ytd_snow, on=merge_criteria)
    # dly_precip_merge = pd.merge(dly_precip_50, dly_precip_75, on=merge_criteria)

    # to_merge = [temp_merge, gdd_merge, mtd_merge, ytd_merge, dly_precip_merge]
    # merged = reduce(lambda left,right: pd.merge(left,right,on=merge_criteria,how='outer'), to_merge)
    # print(merged[100:110])

daily_stations_df()
