import pandas

first_frost_50 = pandas.read_csv('../data/ann-tmin-prbfst-t32Fp50.csv',
        names=['station_id', 'first_frost_50'],
        sep=' ',
        engine='python',
        usecols=[0,7],
        converters={'first_frost_50': lambda x: x[:-1]},
        )
first_frost_90 = pandas.read_csv('../data/ann-tmin-prbfst-t32Fp90.csv',
        names=['station_id', 'first_frost_90'],
        sep=' ',
        engine='python',
        converters={'first_frost_90': lambda x: x[:-1]},
        usecols=[0,7]
        )
last_frost_50 = pandas.read_csv('../data/ann-tmin-prblst-t32Fp50.csv',
        names=['station_id', 'last_frost_50'],
        sep=' ',
        engine='python',
        converters={'last_frost_50': lambda x: x[:-1]},
        usecols=[0,7]
        )
last_frost_90 = pandas.read_csv('../data/ann-tmin-prblst-t32Fp90.csv',
        names=['station_id', 'last_frost_90'],
        sep=' ',
        engine='python',
        converters={'last_frost_90': lambda x: x[:-1]},
        usecols=[0,7]
       )

season_length_50 = pandas.read_csv('../data/ann-tmin-prbgsl-t32Fp50.csv',
        names=['station_id', 'season_length_50'],
        sep=' ',
        engine='python',
        converters={'season_length_50': lambda x: int(x[:-1])},
        usecols=[0,9]
       )
season_length_90 = pandas.read_csv('../data/ann-tmin-prbgsl-t32Fp90.csv',
        names=['station_id', 'season_length_90'],
        sep=' ',
        engine='python',
        converters={'season_length_90': lambda x: int(x[:-1])},
        usecols=[0,9]
       )


gdd_40 = pandas.read_csv('../data/ann-grdd-base40.csv',
        names=['station_id', 'gdd_40'],
        engine='python',
        sep='       ',
        converters={'gdd_40': lambda x: int(x[:-1])}
        )

gdd_50 = pandas.read_csv('../data/ann-grdd-base50.csv',
        names=['station_id', 'gdd_50'],
        engine='python',
        sep='       ',
        converters={'gdd_50': lambda x: int(x[:-1])}
        )
firsts = first_frost_50.merge(first_frost_90, on='station_id')
lasts = last_frost_50.merge(last_frost_90, on='station_id')
seasons = season_length_50.merge(season_length_90, on='station_id')
frosts = firsts.merge(lasts, on='station_id')
frosts_seasons = frosts.merge(seasons, on='station_id')

gdd = gdd_40.merge(gdd_50, on='station_id')
merge = gdd.merge(frosts_seasons, how='left', left_on='station_id', right_on='station_id')

print(merge.tail(25))
