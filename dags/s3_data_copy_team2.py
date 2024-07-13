import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator

# Snowflake connection details
SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_WAREHOUSE = 'BF_ETL0624'
SNOWFLAKE_DATABASE = 'AIRFLOW0624'
SNOWFLAKE_SCHEMA = 'BF_DEV'
SNOWFLAKE_STAGE = 'S3_STAGE_TRANS_ORDER'
SNOWFLAKE_TABLE = 'prestage_staff_info_team2'


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    's3_to_snowflake_incremental_load',
    default_args=default_args,
    description='Load data from S3 to Snowflake incrementally',
    schedule_interval=None,
    start_date=datetime(2024, 7, 13),
    catchup=False,
    tags=['s3', 'snowflake'],
) as dag:

    # Task to load data from S3 to Snowflake for each date
    for date in ['2024-07-13', '2024-07-14', '2024-07-15']:
        s3_key = f'aiflow_project/staff_info_{date.split("-")[1]}{date.split("-")[2]}.csv'
        task_id = f'load_{date}_to_snowflake'
        
        copy_into_prestg = CopyFromExternalStageToSnowflakeOperator(
            task_id=task_id,
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            table=SNOWFLAKE_TABLE,
            schema=SNOWFLAKE_SCHEMA,
            stage=SNOWFLAKE_STAGE,
            file_format='''(type = 'CSV', field_delimiter = ',', skip_header = 1, NULL_IF = ('NULL', 'null', ''), empty_field_as_null = true, FIELD_OPTIONALLY_ENCLOSED_BY = '\"', ESCAPE_UNENCLOSED_FIELD = NONE, RECORD_DELIMITER = '\n')''',
            files=[s3_key],
        )
