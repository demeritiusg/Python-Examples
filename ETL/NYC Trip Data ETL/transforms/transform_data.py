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
