def create_fact_table(par_file, partitioned_s3_location):

	pre_dt = datetime.now(timezone.utc) - timedelta(days=1)
	tlc_tipdata = pq.read_parquet(BytesIO(par_file))

	tlc_tipdata['trip_duration'] = tlc_tipdata['dropoff_datetime'] - tlc_tipdata['pickup_datetime']

	tlc_tipdata.to_parquet(f'fact_table_{pre_dt}.parquet')