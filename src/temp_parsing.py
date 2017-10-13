import csv
import psycopg2
import secrets

conn = psycopg2.connect("dbname=weather_data user=cqlanus password=" + secrets.password)
cur = conn.cursor()

def parseTemp(temp):
    if len(temp) == 4:
        return float(temp[0:2] + '.' + temp[2])
    elif len(temp) == 5 and temp[0].isdigit():
        return float(temp[0:3] + '.' + temp[3])

with open('dly-tmax-normal.csv', 'rb') as csvfile:
    rows = csv.reader(csvfile, delimiter=' ')
    for row in rows:
        rowStr =  ', '.join(row)
        formattedRow =  rowStr.replace(' ,', '').split(', ')
        onlyTemps = formattedRow[2:]
        tempNums = map(parseTemp, onlyTemps)
        month = int(formattedRow[1])
        data = {'stationId': formattedRow[0], 'month': int(formattedRow[1]), 'days': tempNums}
        print data
        cur.execute("INSERT INTO daily_max_temps (station_id, month, days) VALUES (%s, %s, %s)",
                (formattedRow[0], month, tempNums))
        conn.commit()


cur.close()
conn.close()
