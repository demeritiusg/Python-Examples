import os
import boto3
import pandas as pd
from airflow import DAG
#from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from scripts import transform_data
from scripts import email_extract

post_string = 'post_string'

s3_bucket_name = 'bucketName'
s3_bucket_key =''

def task_failure(context):
    """Airflow task failure messagessss"""
    dag_run = context.get('dag_run')
    task_instances = dag_run.get_task_instances()
    print("These task instances failed:", task_instances)
    
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
        python_callable=transform_data,
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