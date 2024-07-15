import os
from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator


SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'AIRFLOW0624'
SNOWFLAKE_SCHEMA = 'BF_DEV'
SNOWFLAKE_ROLE = 'BF_DEVELOPER0624'
SNOWFLAKE_WAREHOUSE = 'BF_ETL0624'

# creating target tables
CREATE_TABLE_SQL = '''
CREATE OR REPLACE TABLE AIRFLOW0624.BF_DEV.dim_Company_Profile_Team4_test (
    ID NUMBER(38,0) PRIMARY KEY,
    SYMBOL VARCHAR(16) UNIQUE,
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

CREATE OR REPLACE TABLE AIRFLOW0624.BF_DEV.dim_Symbols_Team4_test (
    SYMBOL VARCHAR(16) PRIMARY KEY,
    NAME VARCHAR(256),
    EXCHANGE VARCHAR(64)
);

CREATE OR REPLACE TABLE AIRFLOW0624.BF_DEV.fact_Stock_History_Team4_test (
    SYMBOL VARCHAR(16),
    DATE DATE,
    OPEN NUMBER(18,8),
    HIGH NUMBER(18,8),
    LOW NUMBER(18,8),
    CLOSE NUMBER(18,8),
    VOLUME NUMBER(38,8),
    ADJCLOSE NUMBER(18,8),
    PRIMARY KEY (SYMBOL, DATE), 
    FOREIGN KEY (SYMBOL) REFERENCES dim_Symbols_Team4(SYMBOL),
    FOREIGN KEY (SYMBOL) REFERENCES dim_Company_Profile_Team4(SYMBOL)
);
'''

# loading initial data into target tables
# Loading dim table and fat table separately 
SQL_DIM_INSERT_STATEMENT = '''
INSERT INTO AIRFLOW0624.BF_DEV.dim_Company_Profile_Team4_test
SELECT DISTINCT *
FROM US_STOCK_DAILY.DCCM.Company_Profile;

INSERT INTO AIRFLOW0624.BF_DEV.dim_Symbols_Team4_test
SELECT DISTINCT *
FROM US_STOCK_DAILY.DCCM.Symbols;
'''

SQL_FACT_INSERT_STATEMENT = '''
INSERT INTO AIRFLOW0624.BF_DEV.fact_Stock_History_Team4_test
SELECT DISTINCT SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, ADJCLOSE
FROM US_STOCK_DAILY.DCCM.Stock_History;
'''


SQL_INSERT_STATEMENT = '''
INSERT INTO AIRFLOW0624.BF_DEV.dim_Company_Profile_Team4_test
SELECT DISTINCT *
FROM US_STOCK_DAILY.DCCM.Company_Profile;

INSERT INTO AIRFLOW0624.BF_DEV.dim_Symbols_Team4_test
SELECT DISTINCT *
FROM US_STOCK_DAILY.DCCM.Symbols;

INSERT INTO AIRFLOW0624.BF_DEV.fact_Stock_History_Team4_test
SELECT DISTINCT SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, ADJCLOSE
FROM US_STOCK_DAILY.DCCM.Stock_History;
'''

# incremental updates
# To explicitly show pipline dependece, separate the SQL commands into dim_update and fact_update
SQL_DIM_UPDATE_STATEMENT = '''
MERGE INTO AIRFLOW0624.BF_DEV.dim_Company_Profile_Team4_test AS target
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

MERGE INTO AIRFLOW0624.BF_DEV.dim_Symbols_Team4_test AS target
USING (SELECT DISTINCT * FROM US_STOCK_DAILY.DCCM.Symbols) AS source
ON target.SYMBOL = source.SYMBOL
WHEN MATCHED THEN UPDATE SET
    target.NAME = source.NAME,
    target.EXCHANGE = source.EXCHANGE
WHEN NOT MATCHED THEN
    INSERT (SYMBOL, NAME, EXCHANGE)
    VALUES (source.SYMBOL, source.NAME, source.EXCHANGE);
'''

SQL_FACT_UPDATE_STATEMENT = '''
MERGE INTO AIRFLOW0624.BF_DEV.fact_Stock_History_Team4_test AS target
USING (SELECT SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, ADJCLOSE, ROW_NUMBER() OVER (PARTITION BY SYMBOL, DATE ORDER BY SYMBOL, DATE) AS rn
       FROM US_STOCK_DAILY.DCCM.Stock_History
) AS source
ON target.SYMBOL = source.SYMBOL AND target.DATE = source.DATE
WHEN MATCHED AND source.rn = 1 THEN UPDATE SET
    target.OPEN = source.OPEN,
    target.HIGH = source.HIGH,
    target.LOW = source.LOW,
    target.CLOSE = source.CLOSE,
    target.VOLUME = source.VOLUME,
    target.ADJCLOSE = source.ADJCLOSE
WHEN NOT MATCHED AND source.rn = 1 THEN
    INSERT (SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, ADJCLOSE)
    VALUES (source.SYMBOL, source.DATE, source.OPEN, source.HIGH, source.LOW, source.CLOSE, source.VOLUME, source.ADJCLOSE);
'''

SQL_FACT_UPDATE_STATEMENT_ver2 = '''
INSERT INTO AIRFLOW0624.BF_DEV.fact_Stock_History_Team4_test
SELECT SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, ADJCLOSE
FROM US_STOCK_DAILY.DCCM.Stock_History h
WHERE h.DATE > '{}'
'''.format(str(datetime.today().strftime("%Y-%m-%d"))[:10])

with DAG(
    'Project2_snowflake_to_snowflake',
    start_date=datetime(2024, 7, 14),
    schedule='@daily',  
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire'],
    catchup=False,  
) as dag:
    def branch(**kwargs):
        #to determine if a initial set up (create table and insert initial data needs to run)
        date = kwargs['logical_date'].replace(hour=0, minute=0, second=0, microsecond=0)
        print(dag.start_date)
        print(date)
        if date == dag.start_date.replace(hour=0, minute=0, second=0, microsecond=0):
            return 'create_target_tables'
        else:
            return 'skip_set_up_and_loading'
    
    branch_task = BranchPythonOperator(
        task_id = 'determine_branch',
        python_callable=branch,
        provide_context = True
    )
       
    
    create_target_tables_task = SnowflakeOperator(
        task_id='create_target_tables',
        sql=CREATE_TABLE_SQL,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
    )

    load_initial_dim_data_task = SnowflakeOperator(
        task_id='load_dim_initial_data',
        sql=SQL_DIM_INSERT_STATEMENT,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
    )

    load_initial_fact_data_task = SnowflakeOperator(
        task_id='load_fact_initial_data',
        sql=SQL_FACT_INSERT_STATEMENT,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
    )

    skip_initial_set_up = EmptyOperator(
        task_id='skip_set_up_and_loading'
    )

    incremental_dim_update_task = SnowflakeOperator(
        task_id='incremental_dim_update',
        sql=SQL_DIM_UPDATE_STATEMENT,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        #trigger_rule = 'one_success'
    )

    incremental_fact_update_task = SnowflakeOperator(
        task_id='incremental_fact_update',
        sql=SQL_FACT_UPDATE_STATEMENT_ver2,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
    )

    # task dependencies
    #create_target_tables_task >> load_initial_dim_data_task >> load_initial_fact_data_task >> incremental_dim_update_task >> incremental_fact_update_task
    #with branching:
    branch_task >> create_target_tables_task>>load_initial_dim_data_task>>load_initial_fact_data_task
    branch_task >>skip_initial_set_up
    skip_initial_set_up>>incremental_dim_update_task>>incremental_fact_update_task

'''
if __name__ == '__main__':
    dag.test()
'''
