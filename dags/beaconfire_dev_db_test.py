"""
Example use of Snowflake related operators.
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'AIRFLOW0624'
SNOWFLAKE_SCHEMA = 'BF_DEV'
SNOWFLAKE_ROLE = 'BF_DEVELOPER0624'
SNOWFLAKE_WAREHOUSE = 'BF_ETL0624'


DAG_ID = "snowflake_to_snowflake_project2"

# SQL commands to create target tables
CREATE_DIM_COMPANY_PROFILE_SQL = f"""
CREATE OR REPLACE TABLE {SNOWFLAKE_SCHEMA}.DIM_COMPANY_PROFILE_TEAM3 (
    ID NUMBER(38,0),
    SYMBOL VARCHAR(16),
    PRICE NUMBER(18,8),
    BETA NUMBER(18,8),
    VOLAVG NUMBER(38,0),
    MKTCAP NUMBER(38,0),
    LASTDIV NUMBER(18,8),
    RANGE VARCHAR(64),
    CHANGES NUMBER(18,8),
    COMPANYNAME VARCHAR(512),
    EXCHANGE VARCHAR(64),
    INDUSTRY VARCHAR(64),
    WEBSITE VARCHAR(64),
    DESCRIPTION VARCHAR(2048),
    CEO VARCHAR(64),
    SECTOR VARCHAR(64),
    DCFDIFF NUMBER(18,8),
    DCF NUMBER(18,8)
);
"""

CREATE_DIM_SYMBOLS_SQL = f"""
CREATE OR REPLACE TABLE {SNOWFLAKE_SCHEMA}.DIM_SYMBOLS_TEAM3 (
    SYMBOL VARCHAR(16),
    NAME VARCHAR(256),
    EXCHANGE VARCHAR(64)
);
"""

CREATE_FACT_STOCK_HISTORY_SQL = f"""
CREATE OR REPLACE TABLE {SNOWFLAKE_SCHEMA}.FACT_STOCK_HISTORY_TEAM3 (
    SYMBOL VARCHAR(16),
    DATE DATE,
    OPEN NUMBER(18,8),
    HIGH NUMBER(18,8),
    LOW NUMBER(18,8),
    CLOSE NUMBER(18,8),
    VOLUME NUMBER(38,8),
    ADCLOSE NUMBER(18,8)
);
"""

# SQL command to copy data from source to target tables
COPY_TO_DIM_COMPANY_PROFILE_SQL = f"""
INSERT INTO {SNOWFLAKE_SCHEMA}.DIM_COMPANY_PROFILE_TEAM3
SELECT * FROM US_STOCK_DAILY.DCCM.Company_Profile;
"""

COPY_TO_DIM_SYMBOLS_SQL = f"""
INSERT INTO {SNOWFLAKE_SCHEMA}.DIM_SYMBOLS_TEAM3
SELECT * FROM US_STOCK_DAILY.DCCM.Symbols;
"""

COPY_TO_FACT_STOCK_HISTORY_SQL = f"""
INSERT INTO {SNOWFLAKE_SCHEMA}.FACT_STOCK_HISTORY_TEAM3
SELECT * FROM US_STOCK_DAILY.DCCM.Stock_History;
"""

# SQL commands for incremental loading
INCREMENTAL_LOAD_DIM_COMPANY_PROFILE_SQL = f"""
MERGE INTO {SNOWFLAKE_SCHEMA}.DIM_COMPANY_PROFILE_TEAM3 AS target
USING US_STOCK_DAILY.DCCM.Company_Profile AS source
ON target.ID = source.ID
WHEN MATCHED THEN
    UPDATE SET
        target.SYMBOL = source.SYMBOL,
        target.PRICE = source.PRICE,
        target.BETA = source.BETA,
        target.VOLAVG = source.VOLAVG,
        target.MKTCAP = source.MKTCAP,
        target.LASTDIV = source.LASTDIV,
        target.RANGE = source.RANGE,
        target.CHANGES = source.CHANGES,
        target.COMPANYNAME = source.COMPANYNAME,
        target.EXCHANGE = source.EXCHANGE,
        target.INDUSTRY = source.INDUSTRY,
        target.WEBSITE = source.WEBSITE,
        target.DESCRIPTION = source.DESCRIPTION,
        target.CEO = source.CEO,
        target.SECTOR = source.SECTOR,
        target.DCFDIFF = source.DCFDIFF,
        target.DCF = source.DCF
WHEN NOT MATCHED THEN
    INSERT (ID, SYMBOL, PRICE, BETA, VOLAVG, MKTCAP, LASTDIV, RANGE, CHANGES, COMPANYNAME, EXCHANGE, INDUSTRY, WEBSITE, DESCRIPTION, CEO, SECTOR, DCFDIFF, DCF)
    VALUES (source.ID, source.SYMBOL, source.PRICE, source.BETA, source.VOLAVG, source.MKTCAP, source.LASTDIV, source.RANGE, source.CHANGES, source.COMPANYNAME, source.EXCHANGE, source.INDUSTRY, source.WEBSITE, source.DESCRIPTION, source.CEO, source.SECTOR, source.DCFDIFF, source.DCF);
"""

INCREMENTAL_LOAD_DIM_SYMBOLS_SQL = f"""
MERGE INTO {SNOWFLAKE_SCHEMA}.DIM_SYMBOLS_TEAM3 AS target
USING US_STOCK_DAILY.DCCM.Symbols AS source
ON target.SYMBOL = source.SYMBOL
WHEN MATCHED THEN
    UPDATE SET
        target.NAME = source.NAME,
        target.EXCHANGE = source.EXCHANGE
WHEN NOT MATCHED THEN
    INSERT (SYMBOL, NAME, EXCHANGE)
    VALUES (source.SYMBOL, source.NAME, source.EXCHANGE);
"""

INCREMENTAL_LOAD_FACT_STOCK_HISTORY_SQL = f"""
MERGE INTO {SNOWFLAKE_SCHEMA}.FACT_STOCK_HISTORY_TEAM3 AS target
USING US_STOCK_DAILY.DCCM.Stock_History AS source
ON target.SYMBOL = source.SYMBOL AND target.DATE = source.DATE
WHEN MATCHED THEN
    UPDATE SET
        target.OPEN = source.OPEN,
        target.HIGH = source.HIGH,
        target.LOW = source.LOW,
        target.CLOSE = source.CLOSE,
        target.VOLUME = source.VOLUME,
        target.ADCLOSE = source.ADCLOSE
WHEN NOT MATCHED THEN
    INSERT (SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, ADCLOSE)
    VALUES (source.SYMBOL, source.DATE, source.OPEN, source.HIGH, source.LOW, source.CLOSE, source.VOLUME, source.ADCLOSE);
"""

with DAG(
    DAG_ID,
    start_date=datetime(2024, 7, 1),
    schedule_interval='@daily',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['project2'],
    catchup=False,
) as dag:

    create_dim_company_profile = SnowflakeOperator(
        task_id='create_dim_company_profile',
        sql=CREATE_DIM_COMPANY_PROFILE_SQL,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    create_dim_symbols = SnowflakeOperator(
        task_id='create_dim_symbols',
        sql=CREATE_DIM_SYMBOLS_SQL,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    create_fact_stock_history = SnowflakeOperator(
        task_id='create_fact_stock_history',
        sql=CREATE_FACT_STOCK_HISTORY_SQL,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    copy_to_dim_company_profile = SnowflakeOperator(
        task_id='copy_to_dim_company_profile',
        sql=COPY_TO_DIM_COMPANY_PROFILE_SQL,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    copy_to_dim_symbols = SnowflakeOperator(
        task_id='copy_to_dim_symbols',
        sql=COPY_TO_DIM_SYMBOLS_SQL,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    copy_to_fact_stock_history = SnowflakeOperator(
        task_id='copy_to_fact_stock_history',
        sql=COPY_TO_FACT_STOCK_HISTORY_SQL,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    incremental_load_dim_company_profile = SnowflakeOperator(
        task_id='incremental_load_dim_company_profile',
        sql=INCREMENTAL_LOAD_DIM_COMPANY_PROFILE_SQL,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    incremental_load_dim_symbols = SnowflakeOperator(
        task_id='incremental_load_dim_symbols',
        sql=INCREMENTAL_LOAD_DIM_SYMBOLS_SQL,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    incremental_load_fact_stock_history = SnowflakeOperator(
        task_id='incremental_load_fact_stock_history',
        sql=INCREMENTAL_LOAD_FACT_STOCK_HISTORY_SQL,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    (
        create_dim_company_profile >> create_dim_symbols >> create_fact_stock_history >>
        copy_to_dim_company_profile >> copy_to_dim_symbols >> copy_to_fact_stock_history >>
        incremental_load_dim_company_profile >> incremental_load_dim_symbols >> incremental_load_fact_stock_history
    )




