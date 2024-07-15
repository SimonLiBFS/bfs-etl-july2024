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
SNOWFLAKE_STAGE = 'S3_STAGE_TRANS_ORDER'

SNOWFLAKE_CREATE_SQL = '''
create table if not exists prestage_weather_team1_create_test (
	NAME VARCHAR(16777216),
	DATETIME TIMESTAMP_NTZ(9),
	TEMP FLOAT,
	FEELSLIKE FLOAT,
	DEW FLOAT,
	HUMIDITY FLOAT,
	PRECIP FLOAT,
	PRECIPPROB FLOAT,
	PRECIPTYPE VARCHAR(16777216),
	SNOW FLOAT,
	SNOWDEPTH FLOAT,
	WINDGUST FLOAT,
	WINDSPEED FLOAT,
	WINDDIR FLOAT,
	SEALEVELPRESSURE FLOAT,
	CLOUDCOVER FLOAT,
	VISIBILITY FLOAT,
	SOLARRADIATION FLOAT,
	SOLARENERGY FLOAT,
	UVINDEX NUMBER(38,0),
	SEVERERISK NUMBER(38,0),
	CONDITIONS VARCHAR(16777216),
	ICON VARCHAR(16777216),
	STATIONS VARCHAR(16777216),
	DATE DATE
)
'''

CURR_DATE = datetime.utcnow().strftime('%m%d%Y')

with DAG(
    "weather_1_create_copy",
    start_date=datetime(2024, 7, 13),
    end_date = datetime(2024, 7, 16),
    schedule_interval='0 0 * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire_june_de_team1'],
    catchup=True,
) as dag:
    
    create_table = SnowflakeOperator(
        task_id='task_weather_1_create_table',
        sql=SNOWFLAKE_CREATE_SQL,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE
    )

    copy_into_prestg = CopyFromExternalStageToSnowflakeOperator(
        task_id="task_weather_1_data_copy",
        files=["weather_1_{ CURR_DATE }.csv"],
        table='prestage_weather_team1_create_test',
        schema=SNOWFLAKE_SCHEMA,
        stage=SNOWFLAKE_STAGE,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        file_format='''(
            type = 'CSV',
            field_delimiter = ',',
            SKIP_HEADER = 1,
            NULL_IF =('NULL','null',''),
            empty_field_as_null = true,
            FIELD_OPTIONALLY_ENCLOSED_BY = '\"',
            ESCAPE_UNENCLOSED_FIELD = NONE,
            RECORD_DELIMITER = '\n'
        )''',
    )

    create_table >> copy_into_prestg
          
        
    

