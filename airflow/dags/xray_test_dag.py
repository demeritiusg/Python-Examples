from datetime import datetime, timedelta
from sqlalchemy import create_engine
import boto3

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.postgres.operators.postgres import PostgresOperator


post_string = os.environ['post_string']

s3_bucket_name = ''
s3_bucket_key =''


def error_handling(context):
    ti = context['ti']
    err_str = f'task {ti.task_id} has failed at {ti.end_date}'
    return err_str

def sql_run(post_string, sql, **kwargs):
    try:
        engine = create_engine(post_string)
        post_conn = engine.connect()
        data = post_conn.execute(sql)
    except Exception as e:
        return e
    return [dict(d) for d in data]

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

    try:
        engine = create_engine(post_string)
        post_conn = engine.connect()
    except Exception as e:
        return e

    try:
        clean_data.to_csv(f'{d}', index=False)
        clean_data.to_csv(f'{d}_archive.csv', sep='', index=False)
        clean_data.to_sql(f'{d}', post_conn, schema='', if_exists='append', index=False)
    except:
        err_string = f'{d} was not downloaded {ti.task_id} at {ti.end_date}'
        return err_string

with DAG(
    dag_id='xray',
    start_date= datetime(2020, 1, 1),
    schedule_interval='0 0 4 * MON *',
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    catchup=False,
) as dag:

    tvuxZ = EmptyOperator(
        task_id= 'trigger_v22_usa_xray_unzip'
    )

    tvuxT = EmptyOperator(
        task_id= 'trigger_v22_usa_xray_temp'
    )

    tvuxC = EmptyOperator(
        task_id= 'trigger_v22_usa_xray_cron'
    )

    zipped_update_attribute = EmptyOperator(
        task_id= 'update_attribute_zipped'
    )

    update_attribute = EmptyOperator(
        task_id= 'update_attribute'
    )

    zipped_file_fetch_task = EmptyOperator(
        task_id= 'fetch_zip_file'
    )

    zipped_file_fetch_task_failed = PostgresOperator(
        task_id= 'zipped_file_fetch_task_failed',
        sql='',
        trigger_rule=TriggerRule.ONE_FAILED
    )

    unpack_zip_content = EmptyOperator(
        task_id= 'unpack_zip_content'
    )

    file_fetch_task = EmptyOperator(
        task_id= 'fetch_file'
    )

    check_clean_xt = EmptyOperator(
        task_id= 'check_create_xray_table'
    )

    clean_xt = EmptyOperator(
        task_id= 'clean_xray_table'
    )

    convert_record = EmptyOperator(
        task_id= 'convert_record'
    )

    put_database_record = EmptyOperator(
        task_id= 'put_database_record'
    )

    clean_overlay_table = EmptyOperator(
        task_id= 'clean_overlay_table'
    )

    insert_overlay_table = EmptyOperator(
        task_id= 'insert_overaly_table'
    )

    with TaskGroup(group_id='refreshGroup1') as refresh_aircraft_hv:

        rahv_public= EmptyOperator(
            task_id='refresh_aircraft_historical_view'
        )

        rahv_usmc= EmptyOperator(
            task_id='refresh_aircraft_historical_view_marines'
        )

        rahv_usn= EmptyOperator(
            task_id= 'refresh_aircraft_historical_view_navy'
        )

    route_attribute = EmptyOperator(
        task_id= 'route_on_attribute'
    )

    unzipped_pc = EmptyOperator(
        task_id= 'unzipped_process_complete'
    )

    zipped_pc = EmptyOperator(
        task_id= 'zipped_process_complete'
    )

    upload_to_postgres = EmptyOperator(
        task_id= 'upload_to_postgres'
    )

    zipped_file_fetch_task >> unpack_zip_content >> zipped_update_attribute >> zipped_pc

    zipped_file_fetch_task >> zipped_file_fetch_task_failed

    tvuxC >> refresh_aircraft_hv >> route_attribute

    clean_xt << check_clean_xt << file_fetch_task

    file_fetch_task >> update_attribute >> convert_record >> put_database_record >> clean_overlay_table >> insert_overlay_table >> refresh_aircraft_hv >> route_attribute >> unzipped_pc

    upload_to_postgres << [zipped_pc, unzipped_pc]