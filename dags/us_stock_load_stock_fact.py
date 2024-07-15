import os
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_ROLE = 'BF_DEVELOPER0624'
SNOWFLAKE_WAREHOUSE = 'BF_ETL0624'

SOURCE_DATABASE = 'US_STOCK_DAILY'
SOURCE_SCHEMA = 'DCCM'
SOURCE_TABLE_STOCK_HISTORY = 'Stock_History'

TARGET_DATABASE = 'AIRFLOW0624'
TARGET_SCHEMA = 'BF_DEV'
TARGET_TABLE_STOCK_HISTORY = 'FACT_STOCK_HISTORY_TEAM1'

with DAG(
    "us_stock_load_stock_fact",
    start_date=datetime(2024, 7, 13),
    end_date = datetime(2025, 7, 16),
    schedule_interval='0 0 * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire_june_de_team1'],
    catchup=True,
) as dag:
    
    load_stock_current_date = SnowflakeOperator(
        task_id='load_stock_current_date',
        sql=f"""
        INSERT INTO {TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE_STOCK_HISTORY}
        SELECT * FROM {SOURCE_DATABASE}.{SOURCE_SCHEMA}.{SOURCE_TABLE_STOCK_HISTORY}
        WHERE DATE = '{{{{ ds }}}}';
        """,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE,
    )
        
    load_stock_current_date