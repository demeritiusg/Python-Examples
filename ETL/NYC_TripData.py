#gather data from tlc trip record data
#combine parquet files 3 months data

#extract data
#-------
import pandas as pd
import os
import pyarrow.parquet as pq
import urllib.request

urllib.request.urlretrieve('https://d37ci6vzurychx.cloudfront.net/trip-data/...')

parquet_files = ['yellow_tripdata_2023-01.parquet', 'yellow_tripdata_2023-02.parquet', 'yellow_tripdata_2023-03.parquet']

#combine files into 1 dataframe
temp_dataframe = []

for file in parquet_files:
	df = pq.read_table(file).to_pandas()
	temp_dataframe.append(df)

tlc_tipdata = pd.concat(temp_dataframe, ignore_index=True)

"""
transform data
------
1.drop rows with missing data
2.update column names
3.update date column formats
4.add metric columns
"""

tlc_tipdata = tlc_tipdata.dropna()
tlc_tipdata = tlc_tipdata.rename(columns={'tpep_pickup_datetime':'pickup_datetime', 'tpep_dropoff_datetime':'dropoff_datetime'})
tlc_tipdata['pickup_datetime'] = pd.to_datetime(tlc_tipdata['pickup_datetime'])
tlc_tipdata['dropoff_datetime'] = pd.to_datetime(tlc_tipdata['dropoff_datetime'])
tlc_tipdata['trip_duration'] = tlc_tipdata['dropoff_datetime'] - tlc_tipdata['pickup_datetime']

#load data
#-------
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sqlalchemy import exc

sf_account='sf_account'
sf_user='sf_user'
sf_password='sf_password'
sf_database='sf_database'
sf_schema='sf_schema'
sf_warehouse='sf_warehouse'
sf_role='sf_role'

engine = create_engine(URL(
	account=sf_account,
	user=sf_user,
	password=sf_password,
	database=sf_database,
	schema=sf_schema,
	warehouse=sf_warehouse,
	role=sf_role
	))

conn = engine.connect()
try:
	tlc_tipdata('ny_taxi_trips', engine, if_exists='replace', index=False)
except exc.SQLAlchemyError:
	pass
finally:
	conn.close()
	engine.dispose()
