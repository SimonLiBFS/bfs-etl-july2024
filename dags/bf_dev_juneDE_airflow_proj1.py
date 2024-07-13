import os
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator


SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'AIRFLOW0624'
SNOWFLAKE_SCHEMA = 'BF_DEV'
SNOWFLAKE_ROLE = 'BF_DEVELOPER0624'
SNOWFLAKE_WAREHOUSE = 'BF_ETL0624'
#SNOWFLAKE_STAGE = 'TESTING_TEAM4'
SNOWFLAKE_STAGE = 'S3_STAGE_TRANS_ORDER'
SNOWFLAKE_DST_TABLE = 'prestage_UKRailwaySale_Team4'

CREATE_TABLE_SQL_STRING = "CREATE TABLE IF NOT EXISTS prestage_UKRailwaySale_Team4 ( \
TransactionID varchar(250) PRIMARY KEY,\
DateofPurchase DATE,\
TimeofPurchase TIME,\
PurchaseType VARCHAR(50),\
PaymentMethod VARCHAR(250),\
Railcard VARCHAR(50),\
TicketClass VARCHAR(50),\
TicketType VARCHAR(50),\
Price INTEGER,\
DepartureStation VARCHAR(50),\
ArrivalStation VARCHAR(50),\
DateofJourney VARCHAR(50),\
DepartureTime TIME,\
ArrivalTime TIME,\
ActualArrival TIME,\
JourneyStatus VARCHAR(50),\
ReasonForDelay VARCHAR(50),\
RefundRequest BOOLEAN\
)".format(SNOWFLAKE_DST_TABLE)

with DAG(
    "s3_data_copy_test_team4",
    start_date=datetime(2024, 7, 12),
    end_date = datetime(2024, 7, 16),
    schedule='0 */2 * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire'],
    catchup=True,
) as dag:
    
    snowflake_op_sql_str = SnowflakeOperator(
    task_id='snowflake_op_sql_str',
    sql=CREATE_TABLE_SQL_STRING,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA,
    role=SNOWFLAKE_ROLE,
    )

    copy_into_prestg = CopyFromExternalStageToSnowflakeOperator(
        task_id='UKRailwaySale',
        files=['UKRailwaySale_4_07132024.csv'],
        table=SNOWFLAKE_DST_TABLE,
        schema=SNOWFLAKE_SCHEMA,
        stage=SNOWFLAKE_STAGE,
        file_format='''(type = 'CSV', field_delimiter = ',', SKIP_HEADER = 1 \
            NULL_IF =('NULL','null',''), empty_field_as_null = true, FIELD_OPTIONALLY_ENCLOSED_BY = '\"' \
            ESCAPE_UNENCLOSED_FIELD = NONE RECORD_DELIMITER = '\n')''',
    )

    (snowflake_op_sql_str >> copy_into_prestg)

