import csv
import psycopg2
import secrets

def parse(filename, dbName):
    conn = psycopg2.connect("dbname=weather_data user=cqlanus password=" + secrets.password)
    cur = conn.cursor()


    with open(filename, 'rt') as csvfile:
        rows = csv.reader(csvfile, delimiter=' ')
        for row in rows:
            rowStr =  ', '.join(row)
            formattedRow =  rowStr.replace(' ,', '').split(', ')
            stationId = formattedRow[0]
            wpan = stationId[6:];
            onlyTemps = formattedRow[2:]
            tempNums = list(map(parseTemp, onlyTemps))
            month = int(formattedRow[1])
            data = {'stationId': formattedRow[0], 'month': int(formattedRow[1]), 'days': tempNums}
            print(data)
            cur.execute("INSERT INTO " + dbName + " (station_id, wpan, month, days) VALUES (%s, %s, %s, %s)",
                    (stationId, wpan, month, tempNums))
            conn.commit()


    cur.close()
    conn.close()

def parseTemp(temp):
    if len(temp) == 4:
        return float(temp[0:2] + '.' + temp[2])
    elif len(temp) == 5 and temp[0].isdigit():
        return float(temp[0:3] + '.' + temp[3])

parse('../data/dly-tmax-normal.csv','daily_max_temps')
# parse('../data/dly-tmin-normal.csv', 'daily_min_temps')
