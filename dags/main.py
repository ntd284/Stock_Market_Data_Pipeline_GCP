"""Example DAG demonstrating the usage of the BashOperator."""

from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.bash_operator import BashOperator
from google.cloud import storage
from plugin.key.keys import token
import datetime
import pendulum
from plugin.stock_oneday import stock_storage_oneday
from plugin.stock_subscribe import stock_subscribe
from plugin.stock_oneyear import stock_storage
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)

GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID="just-shell-415015"
BUCKET_NAME = 'datastocks123'
CLUSTER_NAME = 'dataproc'
REGION = 'us-central1'
PYSPARK_URI = f'gs://datastocks123/pyspark_gcs_bq.py'

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}


local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

bucket_name = "datastocks123"
source_des_file_name = token.file_gcs()

def send_to_gcs():
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_des_file_name)
    blob.upload_from_filename(source_des_file_name)
    print('upload done')
    return True

default_args = {
    'owner': 'Duong_',
    'depends_on_past': False,
    "start_date": datetime.datetime(2024, 2, 25, tzinfo=local_tz),
    'retries': 3,
    "retry_delay": timedelta(minutes=5),
    "email": ["nguyentuanduong555@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,

}

with DAG(
        default_args=default_args,
        dag_id="daily",
        schedule_interval='0 16 * * 1-5',
        catchup=False
) as dag_SD:
    
    Get_stockinfo_oneday = PythonOperator(
        task_id="Get_stockinfo_oneday",
        python_callable=stock_storage_oneday.stock_oneday
    )

    Send_stockinfo_oneday_gcs = PythonOperator(
        task_id="Send_stockinfo_oneday_gcs",
        python_callable=stock_storage_oneday.send_to_gcs

    )
    daily_suc = EmailOperator(
        task_id="daily_report",
        to="nguyentuanduong555@gmail.com",
        subject="Airflow success alert",
        html_content="""<h1>daily: ok</h1>"""
    )
with DAG(
        default_args=default_args,
        dag_id="hourly",
        schedule_interval='0 10,11,14,15 * * 1-5',
        catchup=False
) as dag_SD:
    
    Get_stockinfo_hourly = PythonOperator(
        task_id="Get_stockinfo_hourly",
        python_callable=stock_subscribe.get_data_realtime
    )
    hourly_suc = EmailOperator(
        task_id="hourly_report",
        to="nguyentuanduong555@gmail.com",
        subject="Airflow success alert",
        html_content="""<h1>hourly_report ok</h1>"""
    )

with DAG(
        default_args=default_args,
        dag_id="yearly",
        schedule_interval='0 15 * * 5',
        catchup=False
) as dag_SD:

    Get_stockinfo_oneyear = PythonOperator(
        task_id="Get_stockinfo_oneyear",
        python_callable=stock_storage.stock_oneyear
    )
    Send_to_gcs = PythonOperator(
        task_id="Send_to_gcs",
        python_callable=send_to_gcs
    )
    
    pyspark_task = DataprocSubmitJobOperator(
        task_id="pyspark_task", job=PYSPARK_JOB, region=REGION, project_id=PROJECT_ID
    )
    yearly_suc = EmailOperator(
        task_id="hourly_report",
        to="nguyentuanduong555@gmail.com",
        subject="Airflow success alert",
        html_content="""<h1>quarter ok</h1>"""
    )

Get_stockinfo_oneday >> Send_stockinfo_oneday_gcs >> daily_suc
Get_stockinfo_hourly >> hourly_suc
Get_stockinfo_oneyear >> Send_to_gcs >> pyspark_task >> yearly_suc