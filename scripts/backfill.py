import sys
import os
import boto3
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
from src.utils.config import load_config
from src.extractors.open_meteo import extract_all_open_meteo_range
from src.transformers.validators import validate_weather_data
from src.transformers.risk_calculator import calculate_fire_risk
from io import BytesIO


if __name__ == "__main__":

    config = load_config(f"{project_root}/configs/config.yaml")
    s3 = boto3.client("s3")

    data = extract_all_open_meteo_range(config, "2023-01-01", "2026-02-01")
    validated_data = validate_weather_data(data)
    risk_data = calculate_fire_risk(validated_data, config)

    for date, day_df in risk_data.groupby("time"):
        year, month, day = date.split("-")
        hive_key = f"gold/fire_risk/year={year}/month={month}/day={day}/risk.parquet"
        parquet_buffer = BytesIO()
        day_df.to_parquet(parquet_buffer)
        parquet_buffer.seek(0)
        s3.put_object(Bucket=config['aws']['bucket'], Key=hive_key, Body=parquet_buffer.getvalue())
        print(f"Uploaded {hive_key}")







