import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

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

# read data from catlog... assumed data has been crawled
read_data = gc.create_dynamic_frame.from_catlog(
	database='events',
	table_name='divisions'
	)

remove_nulls = read_data.toDF()
remove_nulls = remove_nulls.where("'event_category_id' is NOT NULL")

tmp_df = DynamicFrame.fromDF(remove_nulls, gc)
transform_data = tmp_df.applyMapping.apply([
    ('date', 'date'),
    ('event_category_id', 'long'),
    ('event_division', 'string'),
    ('event_start_date', 'date'),
    ('visit_days', 'long'),
    ('DivVisitors', 'long'),
    ('DivCustomers', 'long'),
    ('newstylecount', 'int'),
    ('reusedstylecount', 'int'),
    ('totalstyles', 'int'),
    ('grossdemand', 'float'),
    ('totalskus', 'int'),
    ('vendors', 'int'),
    ('startinginvunits', 'double'),
    ('Customers', 'int'),
    ('COGS', 'int'),
    ('UnitQty', 'int')]
	)

#write the data to s3
gc.write_dynamic_frame.from_options(
	frame = transform_data,
	connection_type = "s3",
	connection_options = {"path": output_path},
	format = 'parquet'
	)

# finish job
job.commit()