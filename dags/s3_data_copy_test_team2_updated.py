import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator

# Snowflake connection details
SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_ROLE = 'BF_DEVELOPER0624'
SNOWFLAKE_WAREHOUSE = 'BF_ETL0624'
SNOWFLAKE_DATABASE = 'AIRFLOW0624'
SNOWFLAKE_SCHEMA = 'BF_DEV'
SNOWFLAKE_STAGE = 'S3_STAGE_TRANS_ORDER'
SNOWFLAKE_TABLE = 'prestage_staff_info_team2'


with DAG(
    "s3_data_copy_test_team2",
    start_date=datetime(2022, 7, 13),
    end_date = datetime(2022, 7, 16),
    schedule_interval='0 6 * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire_airflow_team2'],
    catchup=True,
) as dag:
    
    copy_into_prestg = CopyFromExternalStageToSnowflakeOperator(
        task_id='prestg_staff_info_tram2',
        files=['staff_info_{{ ds[5:7]+ds[8:10]+ds[0:4] }}.csv'],
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        table=SNOWFLAKE_TABLE,
        schema=SNOWFLAKE_SCHEMA,
        stage=SNOWFLAKE_STAGE,
        file_format='''(type = 'CSV', field_delimiter = ',', SKIP_HEADER = 1 \
            NULL_IF =('NULL','null',''), empty_field_as_null = true, FIELD_OPTIONALLY_ENCLOSED_BY = '\"' \
            ESCAPE_UNENCLOSED_FIELD = NONE RECORD_DELIMITER = '\n')''',
    )

    copy_into_prestg