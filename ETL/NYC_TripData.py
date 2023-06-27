#gather data from tlc trip record data
#combine parquet files 3 months data

#extract data
#-------
import pandas as pd
import os
import pyarrow.parquet as pq
import urllib.request
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sqlalchemy import exc



def get_data():
	"""
	download data files from web server and save them into s3 location (future). currently saving 3 
	files into (already downloaded) into folder as examples.

	"""
	parquet_files = ['yellow_tripdata_2023-01.parquet', 'yellow_tripdata_2023-02.parquet', 'yellow_tripdata_2023-03.parquet']

	for file in parquet_files:
		urllib.request.urlretrieve(f'https://d37ci6vzurychx.cloudfront.net/trip-data/{file}')
		
	

def transform_data():
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

	tlc_tipdata.to_parquet()


def load_snowflake():
	"""
	load data
	"""
	pass
