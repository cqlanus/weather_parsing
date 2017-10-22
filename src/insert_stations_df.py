import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from geoalchemy2 import Geometry


import psycopg2 as pg
import create_stations_df as cdf
import secrets
from io import StringIO

address = 'postgresql://cqlanus:' + secrets.password + '@localhost:5432/gardnly2'
engine = create_engine(address)
conn = engine.connect()
Base = declarative_base()

class StationMeta(Base):
  __tablename__ = 'stations'

  id=Column(Integer, primary_key=True)
  station_id=Column(String(255))
  center=Column(Geometry(geometry_type='POINT', srid=4326))
  elevation=Column(Float)
  state=Column(String(255))
  station_name=Column(String(255))
  first_frost_50=Column(String(255))
  first_frost_90=Column(String(255))
  last_frost_50=Column(String(255))
  last_frost_90=Column(String(255))
  season_length_50=Column(Integer)
  season_length_90=Column(Integer)
  gdd_40=Column(Integer)
  gdd_50=Column(Integer)

df = cdf.station_df()
tableToWriteTo = 'stations'
listToWrite = df.to_dict(orient='records')

metadata = sqlalchemy.schema.MetaData(bind=engine, reflect=True)
table = sqlalchemy.Table(tableToWriteTo, metadata, autoload=True)
Session = sessionmaker(bind=engine)
session = Session()

conn.execute(table.delete())
conn.execute(table.insert(), listToWrite)

session.commit()
session.close()