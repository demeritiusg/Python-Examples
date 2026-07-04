import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ['glue_job', 'obj_key'])
obj_key = args['obj_key']

# initializing context and job
sc = SparkContext()
gc = GlueContext(sc)
spark = sc.spark_session
job = Job(gc)
job.init(args['glue_job'], args)

# define source and destination buckets
source_bucket = ''
destination_bucket = ''
source_path = f's3://{source_bucket}/{obj_key}'
output_path = 's3://' + destination_bucket + '/' + obj_key.rsplit(".",1)[0]

# read data from s3 --->  bronze
read_data = gc.create_dynamic_frame.from_options(
	connection_type='s3',
	connection_options={'path': [source_path]},
	format='json',
	format_options={'multiline': False},
	)

#transform into a spark df
spark_df = read_data.toDF()

#drop na values
datasource = spark_df.dropna(thresh=4)

#replcaing missing values
pickup_clean_dataDF = datasource.fillna(value='n/a_ride', subset='tpep_pickup_hour')
dropoff_clean_dataDF = datasource.fillna(value='n/a_ride', subset='tpep_dropoff_hour')


#parsing dates --->  silver
pickup_clean_dataDF.select(hour('pickup_datetime').alias('tpep_pickup_hour'), year('pickup_datetime').alias('tpep_pickup_year')/
                    month('pickup_datetime').alias('tpep_pickup_hour'), weekofyear('pickup_datetime').alias('tpep_pickup_weekday'))


dropoff_clean_dataDF.select(hour('dropoff_datetime').alias('tpep_dropoff_hour'), year('dropoff_datetime').alias('tpep_dropoff_year')/
                    month('dropoff_datetime').alias('tpep_dropoff_month'), weekofyear('dropoff_datetime').alias('tpep_dropoff_weekday'))

# finish job
job.commit()
