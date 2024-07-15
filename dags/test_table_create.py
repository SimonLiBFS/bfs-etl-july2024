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

-- Copy data from source to target
INSERT INTO AIRFLOW0624.BF_DEV.dim_Company_Profile_TEAM2
SELECT * FROM US_STOCK_DAILY.DCCM.Company_Profile;

INSERT INTO AIRFLOW0624.BF_DEV.dim_Symbols_TEAM2
SELECT * FROM US_STOCK_DAILY.DCCM.Symbols;
"""

ALTER_TABLE_SQL = """
CREATE OR REPLACE TABLE AIRFLOW0624.BF_DEV.fact_Stock_History_TEAM2 AS 
SELECT ID,sh.SYMBOL,DATE,OPEN,HIGH,LOW,CLOSE,VOLUME,ADJCLOSE
FROM US_STOCK_DAILY.DCCM.Stock_History sh
JOIN AIRFLOW0624.BF_DEV.dim_Company_Profile_TEAM2 cp
ON sh.SYMBOL = cp.SYMBOL;
"""


with DAG(
        'create_stock_table',
        start_date=datetime(2024, 7, 14),
        end_date = datetime(2024, 7, 15),
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
