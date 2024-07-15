import os
from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'AIRFLOW0624'
SNOWFLAKE_SCHEMA = 'BF_DEV'
SNOWFLAKE_ROLE = 'BF_DEVELOPER0624'
SNOWFLAKE_WAREHOUSE = 'BF_ETL0624'

# creating target tables
CREATE_TABLE_SQL = '''
CREATE OR REPLACE TABLE AIRFLOW0624.BF_DEV.dim_Company_Profile_Team4 (
    ID NUMBER(38,0) PRIMARY KEY,
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

CREATE OR REPLACE TABLE AIRFLOW0624.BF_DEV.dim_Symbols_Team4 (
    SYMBOL VARCHAR(16) PRIMARY KEY,
    NAME VARCHAR(256),
    EXCHANGE VARCHAR(64)
);

CREATE OR REPLACE TABLE AIRFLOW0624.BF_DEV.fact_Stock_History_Team4 (
    SYMBOL VARCHAR(16),
    DATE DATE,
    OPEN NUMBER(18,8),
    HIGH NUMBER(18,8),
    LOW NUMBER(18,8),
    CLOSE NUMBER(18,8),
    VOLUME NUMBER(38,8),
    ADJCLOSE NUMBER(18,8),
    PRIMARY KEY (SYMBOL, DATE), 
    FOREIGN KEY (SYMBOL) REFERENCES dim_Symbols_Team4(SYMBOL)
);
'''

# loading initial data into target tables
SQL_INSERT_STATEMENT = '''
INSERT INTO AIRFLOW0624.BF_DEV.dim_Company_Profile_Team4
SELECT distinct *
FROM US_STOCK_DAILY.DCCM.Company_Profile;

INSERT INTO AIRFLOW0624.BF_DEV.dim_Symbols_Team4 
SELECT distinct *
FROM US_STOCK_DAILY.DCCM.Symbols;

INSERT INTO AIRFLOW0624.BF_DEV.fact_Stock_History_Team4
SELECT distinct *
FROM US_STOCK_DAILY.DCCM.Stock_History;
'''

# incremental updates
SQL_UPDATE_STATEMENT = '''
MERGE INTO AIRFLOW0624.BF_DEV.dim_Company_Profile_Team4 AS target
USING (SELECT DISTINCT * FROM US_STOCK_DAILY.DCCM.Company_Profile) AS source
ON target.ID = source.ID
WHEN MATCHED THEN UPDATE SET
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

MERGE INTO AIRFLOW0624.BF_DEV.dim_Symbols_Team4 AS target
USING (SELECT * FROM US_STOCK_DAILY.DCCM.Symbols) AS source
ON target.SYMBOL = source.SYMBOL
WHEN MATCHED THEN UPDATE SET
    target.NAME = source.NAME,
    target.EXCHANGE = source.EXCHANGE
WHEN NOT MATCHED THEN
    INSERT (SYMBOL, NAME, EXCHANGE)
    VALUES (source.SYMBOL, source.NAME, source.EXCHANGE);

MERGE INTO AIRFLOW0624.BF_DEV.fact_Stock_History_Team4 AS target
USING (SELECT DISTINCT * FROM US_STOCK_DAILY.DCCM.Stock_History) AS source
ON target.SYMBOL = source.SYMBOL AND target.DATE = source.DATE
WHEN MATCHED THEN UPDATE SET
    target.OPEN = source.OPEN,
    target.HIGH = source.HIGH,
    target.LOW = source.LOW,
    target.CLOSE = source.CLOSE,
    target.VOLUME = source.VOLUME,
    target.ADJCLOSE = source.ADJCLOSE
WHEN NOT MATCHED THEN
    INSERT (SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, ADJCLOSE)
    VALUES (source.SYMBOL, source.DATE, source.OPEN, source.HIGH, source.LOW, source.CLOSE, source.VOLUME, source.ADJCLOSE);
'''

with DAG(
    'Project2_snowflake_to_snowflake',
    start_date=datetime(2024, 7, 13),
    schedule_interval='30 * * * *',  
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire'],
    catchup=False,  
) as dag:
    
    create_target_tables_task = SnowflakeOperator(
        task_id='create_target_tables',
        sql=CREATE_TABLE_SQL,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
    )

    load_initial_data_task = SnowflakeOperator(
        task_id='load_initial_data',
        sql=SQL_INSERT_STATEMENT,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
    )

    incremental_update_task = SnowflakeOperator(
        task_id='incremental_update',
        sql=SQL_UPDATE_STATEMENT,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
    )

    # task dependencies
    create_target_tables_task >> load_initial_data_task >> incremental_update_task
