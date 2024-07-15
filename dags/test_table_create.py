"""
Example use of Snowflake related operators.
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'beaconfire'
SNOWFLAKE_SCHEMA = 'dev_db'
SNOWFLAKE_ROLE = 'BF_DEVELOPER0624'
SNOWFLAKE_WAREHOUSE = 'BF_ETL0624'
SNOWFLAKE_STAGE = 'beaconfire_stage'
SNOWFLAKE_SAMPLE_TABLE = 'airflow_testing'


# SQL commands
CREATE_TASK_SQL = """
CREATE OR REPLACE TABLE AIRFLOW0624.BF_DEV.dim_Company_Profile_TEAM2 AS 
SELECT * FROM US_STOCK_DAILY.DCCM.Company_Profile WHERE 1=0;

CREATE OR REPLACE TABLE AIRFLOW0624.BF_DEV.dim_Symbols_TEAM2 AS 
SELECT * FROM US_STOCK_DAILY.DCCM.Symbols WHERE 1=0;

INSERT INTO AIRFLOW0624.BF_DEV.dim_Company_Profile_TEAM2
SELECT * FROM US_STOCK_DAILY.DCCM.Company_Profile;

INSERT INTO AIRFLOW0624.BF_DEV.dim_Symbols_TEAM2
SELECT * FROM US_STOCK_DAILY.DCCM.Symbols;

CREATE OR REPLACE TABLE AIRFLOW0624.BF_DEV.fact_Stock_History_TEAM2 AS 
SELECT ID,sh.SYMBOL as SYMBOL,DATE,OPEN,HIGH,LOW,CLOSE,VOLUME,ADJCLOSE
FROM US_STOCK_DAILY.DCCM.Stock_History sh
JOIN AIRFLOW0624.BF_DEV.dim_Company_Profile_TEAM2 cp
ON sh.SYMBOL = cp.SYMBOL;
"""

ALTER_TABLE_SQL = """
MERGE INTO AIRFLOW0624.BF_DEV.fact_Stock_History_TEAM2 as t
USING (
    SELECT cp.ID as ID, sh.SYMBOL as SYMBOL,DATE,OPEN,HIGH,LOW,CLOSE,VOLUME,ADJCLOSE
    FROM US_STOCK_DAILY.DCCM.Stock_History sh
    JOIN AIRFLOW0624.BF_DEV.dim_Company_Profile_TEAM2 cp
    ON sh.SYMBOL = cp.SYMBOL
) as s
ON t.ID = s.ID
WHEN MATCHED THEN
    UPDATE SET t.SYMBOL = s.SYMBOL, t.DATE = s.DATE,
               t.OPEN = s.OPEN, t.HIGH = s.HIGH,
               t.LOW = s.LOW, t.CLOSE = s.CLOSE,
               t.VOLUME = s.VOLUME, t.ADJCLOSE = s.ADJCLOSE
WHEN NOT MATCHED THEN
    INSERT (ID, SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, ADJCLOSE)
    VALUES (s.ID, s.SYMBOL, s.DATE, s.OPEN, s.HIGH, s.LOW, s.CLOSE, s.VOLUME, s.ADJCLOSE);
"""


with DAG(
        'create_stock_table',
        start_date=datetime(2024, 7, 14),
        end_date = datetime(2024, 7, 17),
        schedule_interval='0 * * * *',
        default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
        tags=['beaconfire_june_de_team2'],
        catchup=True,
) as dag:
    # [START snowflake_example_dag]
    CREATE_TABLE = SnowflakeOperator(
        task_id='create_stock_table',
        sql=CREATE_TASK_SQL,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    UPDATE_TABLE = SnowflakeOperator(
        task_id='update_stock_table',
        sql=ALTER_TABLE_SQL,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    CREATE_TABLE >> UPDATE_TABLE
