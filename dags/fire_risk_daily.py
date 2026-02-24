import sys
sys.path.insert(0, "/opt/airflow")

import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from src.extractors.open_meteo import extract_all_open_meteo
from src.transformers.validators import validate_weather_data
from src.utils.config import load_config
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from io import BytesIO

def extract_and_upload(**kwargs):
    config = load_config()
    data = extract_all_open_meteo(config)
    hook = S3Hook(aws_conn_id="aws_default")
    hook.load_string(
        string_data=json.dumps(data),
        key=f"bronze/weather/{kwargs['ds']}/raw.json",
        bucket_name=config['aws']['bucket'],
        replace=True,
    )
    return f"bronze/weather/{kwargs['ds']}/raw.json"

def validate_and_upload(**kwargs):
    config = load_config()
    hook = S3Hook(aws_conn_id="aws_default")
    s3_key = kwargs["ti"].xcom_pull(task_ids="extract_weather_data")
    json_string = hook.read_key(key=s3_key, bucket_name=config['aws']['bucket'])
    json_data = json.loads(json_string)
    validated_data = validate_weather_data(json_data)
    parquet_buffer = BytesIO()
    validated_data.to_parquet(parquet_buffer)
    parquet_buffer.seek(0)
    hook.load_bytes(
        bytes_data=parquet_buffer.getvalue(),
        key=f"silver/weather/{kwargs['ds']}/clean.parquet",
        bucket_name=config['aws']['bucket'],
        replace=True,
    )
    return f"silver/weather/{kwargs['ds']}/clean.parquet"
    

with DAG(
    dag_id="fire_risk_daily",
    start_date=datetime(2026, 2, 23),
    schedule="@daily",
    catchup=False,
) as dag:
    extract_weather_task = PythonOperator(
        task_id="extract_weather_data",
        python_callable=extract_and_upload,
    )
    validate_weather_task = PythonOperator(
        task_id="validate_weather_data",
        python_callable=validate_and_upload,
    )
    extract_weather_task >> validate_weather_task