import sys

from awsglue.transforms import ApplyMapping
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col

import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)



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

destination_bucket = args.get('destination_bucket', '')
obj_key = args.get('obj_key', '')
output_path = f's3://{destination_bucket}/{obj_key.rsplit(".", 1)[0]}' if obj_key else f's3://{destination_bucket}/output'


source_bucket = args.get('source_bucket', '')

database_name = args.get('database_name', 'events')
table_name = args.get('table_name', 'divisions')

spark_context = SparkContext()
glue_context = GlueContext(spark_context)
job = Job(glue_context)
job.init(args['glue_job'], args)


def read_from_source():
    try:
        read_data = glue_context.create_dynamic_frame.from_catalog(
            database=database_name,
            table_name=table_name,
            transformation_ctx='read_data',
        )
        cleaned_df = read_data.toDF().where(col('event_category_id').isNotNull())
        mapped_frame = DynamicFrame.fromDF(cleaned_df, glue_context, 'mapped_frame')
        
        count = cleaned_df.count()
        logger.info(f"Number of records read from source: {count}")
        
    except Exception:
        logger.exception(f"An error occurred while reading from source")
        raise
    return mapped_frame

    
def transform(mapped_frame):
    try:
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
    except Exception:
        logger.exception("Transformation failed.")
        raise
        
    
    
def write_to_s3(output_path, transform_data):
    
    try:
        if not destination_bucket:
            raise ValueError('destination_bucket argument is required')
    

        glue_context.write_dynamic_frame.from_options(
            frame=transform_data,
            connection_type='s3',
            connection_options={'path': output_path},
            format='parquet',
        )

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise


def main():
    try:
        mapped_frame = read_from_source()
        transform_data = transform(mapped_frame)
        write_to_s3(output_path, transform_data)
        job.commit()
    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise
    
if __name__ == '__main__':
    main()



    




