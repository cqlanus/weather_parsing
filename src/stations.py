import csv
import psycopg2
import secrets

conn = psycopg2.connect("dbname=weather_data user=cqlanus password=" + secrets.password)
cur = conn.cursor()

with open('../../station_sample.csv', 'rb') as csvfile:
    rows = csv.reader(csvfile, delimiter=' ')
    for row in rows:
        rowStr =  ','.join(row)
        usaf =  rowStr[0:6]
        wban = rowStr[7:12]
        stationNameList = rowStr[13:43].split(',')
        stationName = ' '.join(stationNameList).strip()
        country = rowStr[43:45] if rowStr[43] != ',' else None
        state = rowStr[48:50] if rowStr[48] != ',' else None
        callLetters = rowStr[51:55].replace(',', '') if rowStr[51] != ',' else None
        lat = rowStr[57:64] if rowStr[57] != ',' else None
        lng = rowStr[65:73] if rowStr[65] != ',' else None
        elev = rowStr[74:81] if rowStr[74] != ',' else None
        cur.execute("INSERT INTO stations (usaf, wban, station_name, country, state, call_letters, latitude, longitude, elevation) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (usaf, wban, stationName, country, state, callLetters, lat, lng, elev))
        conn.commit()

cur.close()
conn.close()

