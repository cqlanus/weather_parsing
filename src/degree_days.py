import csv
import psycopg2
import secrets

def parse(filename, tableName):
    conn = psycopg2.connect("dbname=gardnly2 user=cqlanus password=" + secrets.password)
    cur = conn.cursor()


    with open(filename, 'rt') as csvfile:
        rows = csv.reader(csvfile, delimiter=' ')
        for row in rows:
          rowArr =  ', '.join(row).replace(' ,', '').split(', ')
          station_id = rowArr[0]
          month = int(rowArr[1])
          heating_units = list(map(parseHeatUnit, rowArr[2:]))
          print(station_id, month, heating_units);
          cur.execute("INSERT INTO " + tableName + " (station_id, month, heating_units) VALUES (%s, %s, %s)",
                    (station_id, month, heating_units))
          conn.commit()

    cur.close()
    conn.close()

def parseHeatUnit(heatUnit):
  if heatUnit[0:2] == '-7':
    return 0
  elif heatUnit[0:2] == '-8':
    return None
  else:
    return int(float(heatUnit[:-1]))


# parse('../data/dly-grdd-base40.csv', 'degree_day_40')
parse('../data/gdd_test.csv', 'degree_day_50')
# parse('../data/dly-grdd-base50.csv', 'degree_day_50')