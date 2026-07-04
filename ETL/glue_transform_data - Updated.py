import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import ApplyMapping
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col

import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

output_path = f's3://{destination_bucket}/{obj_key.rsplit(".", 1)[0]}' if obj_key else f's3://{destination_bucket}/output'


args = getResolvedOptions(
    sys.argv,
    [
        'glue_job',
        'obj_key',
        'source_bucket',
        'destination_bucket',
        'database_name',
        'table_name',
    ],
)
map


obj_key = args.get('obj_key', '')
source_bucket = args.get('source_bucket', '')
destination_bucket = args.get('destination_bucket', '')
database_name = args.get('database_name', 'events')
table_name = args.get('table_name', 'divisions')

sc = SparkContext()
gc = GlueContext(sc)
job = Job(gc)
job.init(args['glue_job'], args)


def read_from_source():
    try:
        read_data = gc.create_dynamic_frame.from_catalog(
            database=database_name,
            table_name=table_name,
            transformation_ctx='read_data',
        )
        cleaned_df = read_data.toDF().where(col('event_category_id').isNotNull())
        mapped_frame = DynamicFrame.fromDF(cleaned_df, gc, 'mapped_frame')
        
    except Exception as e:
        logger.error(f"An error occurred while reading from source: {e}")
        raise
    return cleaned_df, mapped_frame

    
def transform(mapped_frame):
        transform_data = ApplyMapping.apply(
        frame=mapped_frame,
        mappings=[
            ('date', 'date', 'date', 'date'),
            ('event_category_id', 'long', 'event_category_id', 'long'),
            ('event_division', 'string', 'event_division', 'string'),
            ('event_start_date', 'date', 'event_start_date', 'date'),
            ('visit_days', 'long', 'visit_days', 'long'),
            ('DivVisitors', 'long', 'DivVisitors', 'long'),
            ('DivCustomers', 'long', 'DivCustomers', 'long'),
            ('newstylecount', 'int', 'newstylecount', 'int'),
            ('reusedstylecount', 'int', 'reusedstylecount', 'int'),
            ('totalstyles', 'int', 'totalstyles', 'int'),
            ('grossdemand', 'float', 'grossdemand', 'float'),
            ('totalskus', 'int', 'totalskus', 'int'),
            ('vendors', 'int', 'vendors', 'int'),
            ('startinginvunits', 'double', 'startinginvunits', 'double'),
            ('Customers', 'int', 'Customers', 'int'),
            ('COGS', 'int', 'COGS', 'int'),
            ('UnitQty', 'int', 'UnitQty', 'int'),
        ],
        transformation_ctx='transform_data',
    )
        return transform_data
        
    
    
def write_to_s3(output_path, transform_data):
    
    try:
        if not destination_bucket:
            raise ValueError('destination_bucket argument is required')
    

        gc.write_dynamic_frame.from_options(
            frame=transform_data,
            connection_type='s3',
            connection_options={'path': output_path},
            format='parquet',
        )

        job.commit()
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise




    




