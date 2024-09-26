import boto3
import pandas as pd
from sqlalchemy import create_engine, exc

def transform_file(context, s3_bucket, post_string):
    #cleaning data file
    s3_client = boto3.resource('s3')
    bucket = s3_client.Bucket(s3_client)
    ti = context['task_instance']
    updated_filename = ti.xcom_pull(task_id='extract_email_address', key='return_value')
    obj = bucket.Object(s3_bucket, updated_filename)
    data_df = pd.read_csv(obj)
    data_df = data_df.drop_duplicates()
    date_cols = data_df.columns[6:7]
    data_df[date_cols] = data_df[date_cols].apply(pd.to_datetime, format='%Y=%m-%d')
    for col in data_df.columns:
        data_df[col] = data_df[col].str.strip() # this needs to be an append
    clean_data = data_df

    try:
        engine = create_engine(post_string)
        post_conn = engine.connect()
    except exc.SQLAlchemyError as e:
        return e

    try:
        clean_data.to_csv(f'{updated_filename}', index=False)
        clean_data.to_csv(f'{updated_filename}_archive.csv', sep='', index=False)
        clean_data.to_sql(f'{updated_filename}', post_conn, schema='', if_exists='append', index=False)
    except:
        err_string = f'{updated_filename} was not downloaded {ti.task_id} at {ti.end_date}'
        return err_string       