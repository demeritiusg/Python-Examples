#gather data from tlc trip record data
#combine parquet files 3 months data

#extract data
#-------
import pandas as pd
import os
from datetime import datetime, time, timedelta, timezone
import pyarrow.parquet as pq
import urllib.request
from snowflake.sqlalchemy import URL
from io import BytesIO
from sqlalchemy import create_engine
from sqlalchemy import exc

"""
data pipeline:
1. pull data (get_data)
2. load data into bucket 
3. verify data
4. transform data
5. load data into dw
6. final verify
"""

def get_data():
	"""
	download data files from web server and save them into s3 location (future). currently saving 3 
	files into (already downloaded) into folder as examples.
	saves file into daily partitioned files 
	"""
	parquet_files = ['yellow_tripdata_2023-01.parquet', 'yellow_tripdata_2023-02.parquet', 'yellow_tripdata_2023-03.parquet']
	s3_location = s3_location

	for file in parquet_files:
		urllib.request.urlretrieve(f'https://d37ci6vzurychx.cloudfront.net/trip-data/https://d37ci6vzurychx.cloudfront.net/trip-data/{file}', f'{s3_location}')

	

def transform_data(par_file, par_s3_location):
#combine files into 1 dataframe
	parquet_files = par_file
	temp_dataframe = []
	partitioned_s3_location = par_s3_location

	for file in parquet_files:
		df = pq.read_table(file).to_pandas()
		temp_dataframe.append(df)

	tlc_tipdata = pd.concat(temp_dataframe, ignore_index=True)

	"""
	transform data
	------
	1.drop duplicates
	2.update column names
	3.update date column formats
	4.add metric columns
	"""

	tlc_tipdata = tlc_tipdata.drop_duplicates()
	#tlc_tipdata = tlc_tipdata.rename(columns={'tpep_pickup_datetime':'pickup_datetime', 'tpep_dropoff_datetime':'dropoff_datetime'})
	#tlc_tipdata['pickup_datetime'] = pd.to_datetime(tlc_tipdata['pickup_datetime'])
	#tlc_tipdata['dropoff_datetime'] = pd.to_datetime(tlc_tipdata['dropoff_datetime'])
	

	tlc_tipdata['tpep_pickup_hour'] = pd.to_datetime(tlc_tipdata['pickup_datetime']).dt.hour
	tlc_tipdata['tpep_pickup_day'] = pd.to_datetime(tlc_tipdata['pickup_datetime']).dt.day_name()
	tlc_tipdata['tpep_pickup_year'] = pd.to_datetime(tlc_tipdata['pickup_datetime']).dt.year
	tlc_tipdata['tpep_pickup_month'] = pd.to_datetime(tlc_tipdata['pickup_datetime']).dt.month
	tlc_tipdata['tpep_pickup_weekday'] = pd.to_datetime(tlc_tipdata['pickup_datetime']).dt.weekday

	tlc_tipdata['tpep_dropoff_hour'] = pd.to_datetime(tlc_tipdata['dropoff_datetime']).dt.hour
	tlc_tipdata['tpep_dropoff_day'] = pd.to_datetime(tlc_tipdata['dropoff_datetime']).dt.day_name()
	tlc_tipdata['tpep_dropoff_year'] = pd.to_datetime(tlc_tipdata['dropoff_datetime']).dt.year
	tlc_tipdata['tpep_dropoff_month'] = pd.to_datetime(tlc_tipdata['dropoff_datetime']).dt.month
	tlc_tipdata['tpep_dropoff_weekday'] = pd.to_datetime(tlc_tipdata['dropoff_datetime']).dt.weekday	

	tlc_tipdata.to_parquet(f'{partitioned_s3_location}')


def create_fact_table(par_file, partitioned_s3_location):

	pre_dt = datetime.now(timezone.utc) - timedelta(days=1)
	tlc_tipdata = pq.read_parquet(BytesIO(par_file))

	tlc_tipdata['trip_duration'] = tlc_tipdata['dropoff_datetime'] - tlc_tipdata['pickup_datetime']

	tlc_tipdata.to_parquet(f'fact_table_{pre_dt}.parquet')

def update_dim_date():
	pass

def verifiy_dim_locations():
	"""
	verify any new locations
	"""
	pass

def verify_dim_vendor():
	"""
	verify any new vendor ids
	"""
	pass

def verify_dim_payment_types():
	"""
	verify any new payment type codes
	"""
	pass

def load_snowflake():
	"""
	load data
	just trigger snowpipe job? hmmm
	send sns topic (use boto) when data is ready to trigger snowpipe job
	"""
	pass

def write_report():
	"""
	create report for data mart or bi work. load data from sf, create excel 
	"""
