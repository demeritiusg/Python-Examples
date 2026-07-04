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
