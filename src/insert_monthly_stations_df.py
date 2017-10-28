import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import psycopg2 as pg
import create_monthly_stations_df as cdf
import secrets
from io import StringIO

address = 'postgresql://cqlanus:' + secrets.password + '@localhost:5432/gardnly2'
engine = create_engine(address)
conn = engine.connect()
Base = declarative_base()

class MonthlyStations(Base):
  __tablename__ = 'stations_monthly'

  id=Column(Integer, primary_key=True)
  station_id=Column(String(255))
  month=Column(Integer)
  max_temps=Column(ARRAY(Float))
  min_temps=Column(ARRAY(Float))
  daily_gdd_40=Column(ARRAY(Integer))
  daily_gdd_50=Column(ARRAY(Integer))
  mtd_precip=Column(ARRAY(Float))
  mtd_snow=Column(ARRAY(Float))
  ytd_precip=Column(ARRAY(Float))
  ytd_snow=Column(ARRAY(Float))
  daily_precip_50=Column(ARRAY(Float))
  daily_precip_75=Column(ARRAY(Float))


df = cdf.daily_stations_df()
tableToWriteTo = 'stations_monthly'
listToWrite = df.to_dict(orient='records')

metadata = sqlalchemy.schema.MetaData(bind=engine, reflect=True)
table = sqlalchemy.Table(tableToWriteTo, metadata, autoload=True)
Session = sessionmaker(bind=engine)
session = Session()

conn.execute(table.delete())
conn.execute(table.insert(), listToWrite)

session.commit()
session.close()