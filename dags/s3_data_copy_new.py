import os
from datetime import datetime

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
SNOWFLAKE_TABLE = 'prestage_customers_team2'

# SQL to create the table if it does not exist
CREATE_SQL_TABLE="CREATE TABLE IF NOT EXISTS prestage_customers_team2 ( \
firstname varchar(50),\
lastname varchar(50),\
customerid INTEGER,\
orderdate DATE,\
orderid INTEGER,\
email varchar(250),\
gender varchar(50),\
city varchar(250),\
country varchar(50),\
phone varchar(50),\
isgift INTEGER,\
payment varchar(250)\
)".format(SNOWFLAKE_TABLE)

with DAG(
    "s3_customer_data_copy",
    start_date=datetime(2024, 7, 12),
    end_date = datetime(2024, 7, 16),
    schedule_interval='0 * * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire_airflow_team2'],
    catchup=True,
) as dag:
    
    create_table = SnowflakeOperator(
        task_id='create_table',
        sql=CREATE_SQL_TABLE,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )
    
    copy_into_prestg = CopyFromExternalStageToSnowflakeOperator(
        task_id='prestg_customers_team2',
        #files=['customers_team2_07132024.csv'],
        files=['customers_team2_{{ ds[5:7]+ds[8:10]+ds[0:4] }}.csv'],
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        table=SNOWFLAKE_TABLE,
        schema=SNOWFLAKE_SCHEMA,
        stage=SNOWFLAKE_STAGE,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        file_format='''(
            TYPE = 'CSV', 
            FIELD_DELIMITER = ',', 
            SKIP_HEADER = 1,
            NULL_IF = ('NULL','null',''), 
            EMPTY_FIELD_AS_NULL = TRUE, 
            FIELD_OPTIONALLY_ENCLOSED_BY = '"', 
            ESCAPE_UNENCLOSED_FIELD = NONE, 
            RECORD_DELIMITER = '\n',
            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
        )''',
    )

    create_table >> copy_into_prestg
