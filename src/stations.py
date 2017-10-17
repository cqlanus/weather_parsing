import csv
import psycopg2
import secrets
import re

conn = psycopg2.connect("dbname=gardnly2 user=cqlanus password=" + secrets.password)
cur = conn.cursor()

with open('../data/allstations.csv', 'rt') as csvfile:
    rows = csv.reader(csvfile, delimiter=' ')
    for row in rows:
        rowStr =  ','.join(row)
        # print(rowStr)
        station_id = rowStr[0:11]
        wban = station_id[5:11]
        lat = rowStr[12:19].replace(',', '')
        lng = rowStr[21:30].replace(',', '')
        coordinates = "POINT(%s %s)" % (lat, lng)
        elevation = float(rowStr[31:37].replace(',',''))
        state = rowStr[38:40]
        name = rowStr[41:]
        newname = re.sub('[0-9]', ',', name)
        newname2 = re.sub(',,[A-Z]{3},,', ',', newname)
        newname3 = re.sub(',+$', '', newname2).replace(',',' ')

        cur.execute("INSERT INTO stations (station_id, wban, station_name, state, center, elevation) VALUES (%s, %s, %s, %s, %s, %s)",
                    (station_id, wban, newname3, state, coordinates, elevation))
        conn.commit()


cur.close()
conn.close()

