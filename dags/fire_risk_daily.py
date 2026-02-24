import sys
sys.path.insert(0, "/opt/airflow")

import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from src.extractors.open_meteo import extract_all_open_meteo
from src.utils.config import load_config
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def extract_and_upload_weather_data(**kwargs):
    config = load_config()
    data = extract_all_open_meteo(config)
    hook = S3Hook(aws_conn_id="aws_default")
    hook.load_string(
        string_data=json.dumps(data),
        key=f"bronze/weather/{kwargs['ds']}/raw.json",
        bucket_name=config['aws']['bucket'],
    )

with DAG(
    dag_id="fire_risk_daily",
    start_date=datetime(2026, 2, 23),
    schedule="@daily",
    catchup=False,
) as dag:
    extract_weather_data = PythonOperator(
        task_id="extract_weather_data",
        python_callable=extract_and_upload_weather_data,
    )