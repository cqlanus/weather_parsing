import csv
import psycopg2
import secrets


def parse(filename, tableName):

  conn = psycopg2.connect("dbname=gardnly2 user=cqlanus password=" + secrets.password)
  cur = conn.cursor()

  with open(filename, 'rt') as csvfile:
    rows = csv.reader(csvfile)
    for row in rows:
      zip = row[0]
      latitude = row[1]
      longitude = row[2] if row[2][0] != '+' else row[2][1:]
      coordinates = "POINT(%s %s)" % (latitude, longitude)
      cur.execute("INSERT INTO " + tableName + "(zip, center) VALUES (%s, %s)",
                   (zip, coordinates))
      conn.commit()

  cur.close()
  conn.close()


parse('../data/zipcodes.csv', 'zips')