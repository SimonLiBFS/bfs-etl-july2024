"""
Example use of Snowflake related operators.
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator


SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'airflow0624'
SNOWFLAKE_SCHEMA = 'bf_dev'

SNOWFLAKE_ROLE = 'bf_developer0624'
SNOWFLAKE_WAREHOUSE = 'bf_etl0624'
SNOWFLAKE_STAGE = 's3_stage_trans_order'

with DAG(
    "s3_data_copy_test",
    start_date=datetime(2024, 7, 13),
    end_date = datetime(2024, 7, 16),
    schedule_interval='0 7 * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire'],
    catchup=True,
) as dag:

    copy_into_prestg = CopyFromExternalStageToSnowflakeOperator(
        task_id='team3_project1',
        files=['atp_team3_{{ ds[5:7]+ds[8:10]+ds[0:4] }}.csv'],
        table='prestage_atp_team3',
        role=SNOWFLAKE_ROLE,
        schema=SNOWFLAKE_SCHEMA,
        stage=SNOWFLAKE_STAGE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        file_format='''(type = 'CSV', field_delimiter = ',', SKIP_HEADER = 1 \
            NULL_IF =('NULL','null',''), empty_field_as_null = true, FIELD_OPTIONALLY_ENCLOSED_BY = '\"' \
            ESCAPE_UNENCLOSED_FIELD = NONE RECORD_DELIMITER = '\n')''',
    )

    copy_into_prestg
          
        
    

