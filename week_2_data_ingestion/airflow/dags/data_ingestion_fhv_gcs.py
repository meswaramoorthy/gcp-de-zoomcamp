# DAG to download nyc taxi FHV data and upload to gcs

# Imports

import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from google.cloud import storage
import pyarrow.csv as pv
import pyarrow.parquet as pq

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", '/opt/airflow/')
# path_to_creds = f"{AIRFLOW_HOME}/google_credentials.json"
path_to_creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

PROJECT_ID= os.environ.get("GCP_PROJECT_ID")
BUCKET= os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

URL_PREFIX= 'https://s3.amazonaws.com/nyc-tlc/trip+data'
FILE_NAME= 'fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
URL_TEMPLATE= URL_PREFIX + '/' + FILE_NAME
OUTPUT_FILE_TEMPLATE= AIRFLOW_HOME + '/output_fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'


def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in csv format.")
        return
    
    table= pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))    

default_args= {
    "owner": "airflow",    
    "depends_on_past": False,
    "retries": 1,
}

# DAG Declaration
with DAG(
    dag_id= "nyc_taxidata_fhv_ingestion_gcs",
    schedule_interval= "@monthly",  
    start_date= datetime(2019, 1, 1),
    end_date= datetime(2021, 7, 30),
    default_args= default_args,
    catchup= True,
    max_active_runs= 1,
    tags= ["dtc-de-nyc-tripdata-fhv"],
) as dag:

    download_dataset_task = BashOperator(
        task_id= "download_dataset_task",
        bash_command= f"curl -sSLf {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}" # Adding f option to make curl fail for 404
    )

    format_to_parquet_task= PythonOperator(
        task_id= "format_to_parquet_task",
        python_callable= format_to_parquet,
        op_kwargs= {
            "src_file": OUTPUT_FILE_TEMPLATE
        },
    )

    upload_to_gcs_bash_task = BashOperator(
        task_id = "upload_to_gcs_bash_task",
        bash_command = f"ls && \
            gcloud auth activate-service-account --key-file={path_to_creds} && \
        gsutil -m cp {OUTPUT_FILE_TEMPLATE.replace('.csv', '.parquet')} gs://{BUCKET}/raw",
    )

    remove_local_file_task = BashOperator(
        task_id = "remove_local_file_task",
        bash_command = f"rm {OUTPUT_FILE_TEMPLATE} {OUTPUT_FILE_TEMPLATE.replace('.csv', '.parquet')}"
    )

    download_dataset_task >> format_to_parquet_task >> upload_to_gcs_bash_task >> remove_local_file_task