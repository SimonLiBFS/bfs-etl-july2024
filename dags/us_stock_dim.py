import os
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator


SNOWFLAKE_CONN_ID = 'snowflake_conn'

SOURCE_DATABASE = 'US_STOCK_DAILY'
SOURCE_SCHEMA = 'DCCM'
SOURCE_TABLE_COMPANY_PROFILE = 'Company_Profile'
SOURCE_TABLE_STOCK_HISTORY = 'Stock_History'
SOURCE_TABLE_SYMBOLS = 'Symbols'


TARGET_DATABASE = 'AIRFLOW0624'
TARGET_SCHEMA = 'BF_DEV'
TARGET_TABLE_COMPANY_PROFILE = 'DIM_COMPANY_PROFILE_TEAM1'
TARGET_TABLE_STOCK_HISTORY = 'FACT_STOCK_HISTORY_TEAM1'
TARGET_TABLE_SYMBOLS = 'DIM_SYMBOLS_TEAM1'

SNOWFLAKE_ROLE = 'BF_DEVELOPER0624'
SNOWFLAKE_WAREHOUSE = 'BF_ETL0624'
SNOWFLAKE_STAGE = 'S3_STAGE_TRANS_ORDER'


with DAG(
    "us_stock_dim",
    start_date=datetime(2024, 7, 13),
    schedule_interval='0 0 * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire_june_de_team1'],
    catchup=True,
) as dag:

    create_company_profile_table = SnowflakeOperator(
        task_id='create_company_profile_table',
        sql=f"""
        CREATE TABLE IF NOT EXISTS {TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE_COMPANY_PROFILE} AS
        SELECT * FROM {SOURCE_DATABASE}.{SOURCE_SCHEMA}.{SOURCE_TABLE_COMPANY_PROFILE};
        """,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE,
    )
    
    create_symbols_table = SnowflakeOperator(
        task_id='create_symbols_table',
        sql=f"""
        CREATE TABLE IF NOT EXISTS {TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE_SYMBOLS} AS
        SELECT * FROM {SOURCE_DATABASE}.{SOURCE_SCHEMA}.{SOURCE_TABLE_SYMBOLS};
        """,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE,
    )
    
    create_company_profile_table >> create_symbols_table