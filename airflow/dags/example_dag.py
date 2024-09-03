import os
import boto3
import pandas as pd
from airflow import DAG
#from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
#from sqlalchemy import create_engine

post_string = 'post_string'

s3_bucket_name = 'bucketName'
s3_bucket_key =''

def task_failure(context):
    """Airflow task failure messagessss"""
    dag_run = context.get('dag_run')
    task_instances = dag_run.get_task_instances()
    print("These task instances failed:", task_instances)

# def sql_run(post_string, sql, **kwargs):
#     try:
#         engine = create_engine(post_string)
#         post_conn = engine.connect()
#         data = post_conn.execute(sql)
#     except Exception as e:
#         return e
#     return [dict(d) for d in data]

def email_extract(filename, s3_bucket):
    #add yyyy_MM_dd_HH_mm_ss to filename
    #lowercase filename
    s3_client = boto3.resource('s3')
    bucket = s3_client.Bucket(s3_bucket)
    now = datetime.now()
    ending = now.strftime("Y_%m_%d_%H_%S")
    #name = file
    for name in bucket.glob('*[!0123456789]/.csv'):
        new_path = name.with_stem(name.stem + ending)
        if not new_path.exists():
            newFileName = name.rename(new_path)
            newFileName = newFileName.lower()
            return newFileName
        
def transform_file(context, s3_bucket, post_string):
    #cleaning data file
    s3_client = boto3.resource('s3')
    bucket = s3_client.Bucket(s3_client)
    ti = context['task_instance']
    d = ti.xcom_pull(task_id='extract_email_address', key='return_value')
    obj = bucket.Object(s3_bucket, d)
    data_df = pd.read_csv(obj)
    data_df = data_df.drop_duplicates()
    date_cols = data_df.columns[6:7]
    data_df[date_cols] = data_df[date_cols].apply(pd.to_datetime, format='%Y=%m-%d')
    for col in data_df.columns:
        data_df[col] = data_df[col].str.strip() # this needs to be an append
    clean_data = data_df

    # try:
    #     engine = create_engine(post_string)
    #     post_conn = engine.connect()
    # except Exception as e:
    #     return e

    try:
        clean_data.to_csv(f'{d}', index=False)
        clean_data.to_csv(f'{d}_archive.csv', sep='', index=False)
        # clean_data.to_sql(f'{d}', post_conn, schema='', if_exists='append', index=False)
    except:
        err_string = f'{d} was not downloaded {ti.task_id} at {ti.end_date}'
        return err_string
    
with DAG(
    dag_id='example_dag',
    description='some dags description',
    start_date=datetime(2020, 1, 1),
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': '',
        'provide_contenxt': True
    },
    catchup=False
)   as dag:

    t1 = EmptyOperator(
        task_id='Email_sensor_trigger_dag'
    )

    t3 = PythonOperator(
        task_id='extract_email_attributes',
        python_callable=email_extract,
        provide_context=True
    )

    t9 = PythonOperator(
        task_id='task_parser',
        python_callable=transform_file,
        provide_context=True
    )

    # t14 = PythonOperator(
    #     task_id='copy_stage_table',
    #     python_callable=sql_run,
    #     provide_context=True,
    #     op_kwargs={'sql': 'truncate table data; insert into data; select * from staging_data;'}
    # )

    # t15 = PythonOperator(
    #     task_id='complete',
    #     python_callable=sql_run,
    #     provide_context=True,
    #     op_kwargs={'sql':'select count(*), getdate from data'}
    # )

    t1 >> t3 >> t9