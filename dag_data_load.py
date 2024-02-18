from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
import requests
import json

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 17),
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

def fetch_data_from_api():
    url = "https://fakestoreapi.com/products"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data from API. Status code: {response.status_code}")

def upload_to_s3(data, key, bucket_name):
    now = datetime.now()
    actual_time = now.strftime("%H%M%S")
    key = f"products{actual_time}.json"

    s3 = S3Hook(aws_conn_id='aws_default')
    s3.load_string(string_data=json.dumps(data), key=key, bucket_name=bucket_name, replace=True)

with DAG('api_to_s3',
         default_args=default_args,
         description='DAG to fetch data from API and upload to S3',
         schedule_interval=timedelta(seconds=15),
         catchup=False) as dag:

    fetch_data = PythonOperator(
        task_id='fetch_data_from_api',
        python_callable=fetch_data_from_api
    )

    upload_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_args=["{{ ti.xcom_pull(task_ids='fetch_data_from_api') }}", "products.json", "shoppy123"]
    )

    fetch_data >> upload_s3