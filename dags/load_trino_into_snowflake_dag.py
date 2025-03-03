import snowflake.connector
from snowflake.snowpark import Session
import trino
import logging
from include.eczachly.trino_queries import execute_trino_query, run_trino_dq
from include.eczachly.aws_secret_manager import get_secret
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
import os
import requests
from dotenv import load_dotenv
from ast import literal_eval

logger = logging.getLogger(__name__)
# The way we're doing the DQ check here is
def run_trino_query_dq_check(query):
    results = execute_trino_query(query)
    if len(results) == 0:
        raise ValueError('The query returned no results!')
    for result in results:
        for column in result:
            if type(column) is bool:
                assert column is True

load_dotenv()


schema = 'bigchungus0148'

@dag(
    description="A dag that loads data from Trino into Snowflake",
    default_args={  
        "owner": schema,
        "start_date": datetime(2025, 2, 25),
        "retries": 0,
        "execution_timeout": timedelta(hours=1),
    },
    max_active_runs=1,
    schedule_interval=None,
    catchup=False,
    tags=["community"],
)

def load_polygon_into_snowflake_dag():

    def execute_trino_query(query):
        conn = trino.dbapi.connect(
            host='',
            port=,
            user='',
            http_scheme='',
            catalog='',
            auth=trino.auth.BasicAuthentication(''),
        )
        print(query)
        cursor = conn.cursor()
        print("Executing query for the first time...")
        cursor.execute(query)
        return cursor.fetchall()
                
    connection_params = {
        "account": '',
        "user": '',
        "password": '',
        "role": "",
        'warehouse': '',
        'database': ''
    }


    def get_snowpark_session(schema=''):
        connection_params['schema'] = schema
        session = Session.builder.configs(connection_params).create()
        return session

    def get_data_and_schema_from_trino(table='bigchungus0148.stock_prices_version5'):
        # Define Snowflake connection parameters
        # Create a Snowpark session
        snowflake_session = get_snowpark_session('bigchungus0148')
        data = execute_trino_query(f'SELECT * FROM {table}')
        print('We found {} rows'.format(len(data)))
        schema = execute_trino_query(f'DESCRIBE {table}')
        columns = []
        column_names = []
        for column in schema:
            column_names.append(column[0])
            columns.append(' '.join(column))
        current_config = snowflake_session.sql("SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()").collect()
        print(f"Using warehouse: {current_config[0][0]}, database: {current_config[0][1]}, schema: {current_config[0][2]}")
        columns_str = ','.join(columns)
        create_ddl = f'CREATE TABLE OR REPLACE {table} ({columns_str})'
        print(create_ddl)
        snowflake_session.sql(create_ddl)
        write_df = snowflake_session.create_dataframe(data, schema=column_names)
        write_df.write.mode("overwrite").save_as_table(table)
        snowflake_session.close()


    load_to_snowflake_step = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=get_data_and_schema_from_trino,
    )

    load_to_snowflake_step

load_polygon_into_snowflake_dag()