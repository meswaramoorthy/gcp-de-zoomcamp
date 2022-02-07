# Imports
import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from google.cloud import storage
import pyarrow.csv as pv
import pyarrow.parquet as pq

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", '/opt/airflow/')
PROJECT_ID= os.environ.get("GCP_PROJECT_ID")
BUCKET= os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

URL_PREFIX= 'https://s3.amazonaws.com/nyc-tlc/misc'
FILE_NAME= 'taxi+_zone_lookup.csv'
URL_TEMPLATE= URL_PREFIX + '/' + FILE_NAME
OUTPUT_FILE_TEMPLATE= AIRFLOW_HOME + '/output_' + FILE_NAME

path_to_creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in csv format.")
        return
    
    table= pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))    

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround
    # file_name= ti.xcom_pull(task_ids= prev_task_id, key= xcom_key)
    logging.info(bucket + ':' + object_name + ':' + local_file)
    print(PROJECT_ID , ':' , BIGQUERY_DATASET , ':' , FILE_NAME.replace('.csv', '.parquet'))
    
    client= storage.Client()
    bucket= client.bucket(bucket)

    blob= bucket.blob(object_name)
    blob.upload_from_filename(local_file)

default_args= {
    "owner": "airflow",    
    "depends_on_past": False,
    "retries": 1,
}

# DAG Declaration

with DAG(
    dag_id= "data_ingestion_zonelookup_gcs",
    schedule_interval= "@daily",  
    start_date= days_ago(1),
    # end_date= datetime(2021, 7, 30),
    default_args= default_args,
    catchup= False,
    max_active_runs= 1,
    tags= ["dtc-de-nyc-zonelookup"],
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

    # local_to_gcs_task = PythonOperator(
    #     task_id= "local_to_gcs_task",
    #     python_callable= upload_to_gcs,
    #     op_kwargs= {
    #         "bucket": BUCKET,
    #         "object_name": f"raw/{FILE_NAME.replace('.csv', '.parquet')}",
    #         "local_file": OUTPUT_FILE_TEMPLATE.replace('.csv', '.parquet'),
    #         # "prev_task_id": "format_to_parquet_task",
    #         # "xcom_key": "parquet_file_name"
    #     }
    # )

    upload_to_gcs_bash_task = BashOperator(
        task_id = "upload_to_gcs_bash_task",
        bash_command = f"ls && gcloud auth activate-service-account --key-file={path_to_creds} && \
        gsutil -m cp {OUTPUT_FILE_TEMPLATE.replace('.csv', '.parquet')} gs://{BUCKET}/raw",
    )

    # bigquery_external_table_task = BigQueryCreateExternalTableOperator(
    #     task_id = 'bigquery_external_table_task',
    #     table_resource = {
    #         "tableReference": {
    #             "projectId": PROJECT_ID,
    #             "datasetId": BIGQUERY_DATASET,
    #             "tableId": "external_table"
    #         },
    #         "externalDataConfiguration": {
    #             "sourceFormat" : "PARQUET",
    #             "sourceUris": [f"gs://{BUCKET}/raw/{FILE_NAME.replace('.csv', '.parquet')}"],
    #         },
    #     },
    # )

    remove_local_file_task = BashOperator(
        task_id = "remove_local_file_task",
        bash_command = f"rm {OUTPUT_FILE_TEMPLATE} {OUTPUT_FILE_TEMPLATE.replace('.csv', '.parquet')}"
    )

download_dataset_task >> format_to_parquet_task >> upload_to_gcs_bash_task >> remove_local_file_task