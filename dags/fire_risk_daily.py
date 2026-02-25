import sys
sys.path.insert(0, "/opt/airflow")

import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from src.extractors.open_meteo import extract_all_open_meteo
from src.transformers.validators import validate_weather_data
from src.transformers.risk_calculator import calculate_fire_risk
from src.utils.config import load_config
from src.alerts.sns_alert import sns_alert
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from io import BytesIO
import pandas as pd

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
    validated_data = validated_data[validated_data["time"] == kwargs["ds"]]
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

def calculate_risk_and_upload(**kwargs):
    config = load_config()
    hook = S3Hook(aws_conn_id="aws_default")
    s3_key = kwargs["ti"].xcom_pull(task_ids="validate_weather_data")
    parquet_data = hook.get_key(key=s3_key, bucket_name=config['aws']['bucket'])
    df = pd.read_parquet(BytesIO(parquet_data.get()["Body"].read()))
    df_risk = calculate_fire_risk(df, config)
    parquet_buffer = BytesIO()
    df_risk.to_parquet(parquet_buffer)
    parquet_buffer.seek(0)
    ds = kwargs["ds"]
    year, month, day = ds.split("-")
    hive_key = f"gold/fire_risk/year={year}/month={month}/day={day}/risk.parquet"
    hook.load_bytes(
        bytes_data=parquet_buffer.getvalue(),
        key=hive_key,
        bucket_name=config['aws']['bucket'],
        replace=True,
    )
    return hive_key

def check_and_alert(**kwargs):
    config = load_config()
    hook = S3Hook(aws_conn_id="aws_default")
    s3_key = kwargs["ti"].xcom_pull(task_ids="calculate_fire_risk")
    parquet_data = hook.get_key(key=s3_key, bucket_name=config['aws']['bucket'])
    df = pd.read_parquet(BytesIO(parquet_data.get()["Body"].read()))
    sns_alert(df, config)
    

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
    calculate_risk_task = PythonOperator(
        task_id="calculate_fire_risk",
        python_callable=calculate_risk_and_upload,
    )
    check_and_alert_task = PythonOperator(
        task_id="check_and_alert",
        python_callable=check_and_alert,
    )
    extract_weather_task >> validate_weather_task >> calculate_risk_task >> check_and_alert_task