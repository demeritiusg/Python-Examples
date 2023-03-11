from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.postgres.operators.postgres import PostgresOperator

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