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
    "us_stock",
    start_date=datetime(2024, 7, 13),
    schedule_interval=None,
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire_june_de_team1'],
) as dag:

    create_company_profile_table = SnowflakeOperator(
        task_id='create_company_profile_table',
        sql=f"""
        CREATE TABLE IF NOT EXISTS {TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE_COMPANY_PROFILE} AS
        SELECT * FROM {SOURCE_DATABASE}.{SOURCE_SCHEMA}.{SOURCE_TABLE_COMPANY_PROFILE} WHERE 1=0;
        """,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE,
    )

    copy_company_profile_data = SnowflakeOperator(
        task_id='copy_company_profile_data',
        sql=f"""
        INSERT INTO {TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE_COMPANY_PROFILE}
        SELECT * FROM {SOURCE_DATABASE}.{SOURCE_SCHEMA}.{SOURCE_TABLE_COMPANY_PROFILE};
        """,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE,
    )
    
    create_stock_history_table = SnowflakeOperator(
        task_id='create_stock_history_table',
        sql=f"""
        CREATE TABLE IF NOT EXISTS {TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE_STOCK_HISTORY} AS
        SELECT * FROM {SOURCE_DATABASE}.{SOURCE_SCHEMA}.{SOURCE_TABLE_STOCK_HISTORY} WHERE 1=0;
        """,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE,
    )

    copy_stock_history_data = SnowflakeOperator(
        task_id='copy_stock_history_data',
        sql=f"""
        INSERT INTO {TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE_STOCK_HISTORY}
        SELECT * FROM {SOURCE_DATABASE}.{SOURCE_SCHEMA}.{SOURCE_TABLE_STOCK_HISTORY};
        """,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE,
    )
    
    
    create_symbols_table = SnowflakeOperator(
        task_id='create_symbols_table',
        sql=f"""
        CREATE TABLE IF NOT EXISTS {TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE_SYMBOLS} AS
        SELECT * FROM {SOURCE_DATABASE}.{SOURCE_SCHEMA}.{SOURCE_TABLE_SYMBOLS} WHERE 1=0;
        """,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE,
    )
    
    copy_symbols_data = SnowflakeOperator(
        task_id='copy_symbols_data',
        sql=f"""
        INSERT INTO {TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE_SYMBOLS}
        SELECT * FROM {SOURCE_DATABASE}.{SOURCE_SCHEMA}.{SOURCE_TABLE_SYMBOLS};
        """,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE,
    )
    
    create_company_profile_table >> copy_company_profile_data >> create_stock_history_table >> copy_stock_history_data >> create_symbols_table >> copy_symbols_data