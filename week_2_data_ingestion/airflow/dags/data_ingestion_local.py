import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from data_ingestion_script import ingest_callable_task
from data_ingestion_script import my_callable_func


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", '/opt/airflow/')

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

# PG_HOST = os.environ.get('PG_HOST', 'pgdatabase')
# PG_USER = os.environ.get('PG_USER', 'root')
# PG_PASSWORD = os.environ.get('PG_PASSWORD', 'root')
# PG_PORT = os.environ.get('PG_PORT', '5432')
# PG_DATABASE = os.environ.get('PG_DATABASE', 'ny_taxi')

URL_PREFIX= 'https://s3.amazonaws.com/nyc-tlc/trip+data'
URL_TEMPLATE= URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
OUTPUT_FILE_TEMPLATE= AIRFLOW_HOME + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
TABLE_NAME_TEMPLATE= 'yellow_taxi_trip_{{ execution_date.strftime(\'%Y_%m\') }}'

local_workflow = DAG(
    dag_id= "LocalIngestionDAG",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2021, 1, 1)
)

with local_workflow:
    wget_task= BashOperator(
        task_id= "wget_task",
        bash_command= f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}'
        # bash_command= f'echo {URL_TEMPLATE} {OUTPUT_FILE_TEMPLATE}'
    )

    # ingest_task= BashOperator(
    #     task_id= 'ingest_task',
    #     bash_command= f'ls {AIRFLOW_HOME} && wc -l {OUTPUT_FILE_TEMPLATE}'
    #     # bash_command= f'ls {AIRFLOW_HOME}'
    # )

    
    # 

    ingest_task= PythonOperator(
        task_id= 'ingest_task',
        python_callable= ingest_callable_task,
        op_kwargs= dict(user=PG_USER,
         password=PG_PASSWORD,
         host=PG_HOST,
         port=PG_PORT,
         db=PG_DATABASE,
         table_name=TABLE_NAME_TEMPLATE,
         csv_file=OUTPUT_FILE_TEMPLATE
         )
    )
    

    wget_task >> ingest_task

