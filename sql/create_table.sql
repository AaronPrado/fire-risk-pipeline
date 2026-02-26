-- Create external table over Hive-partitioned Parquet files in S3 Gold layer
CREATE EXTERNAL TABLE fire_risk.daily_risk (
    time STRING,
    location STRING,
    temperature_2m_max DOUBLE,
    temperature_2m_min DOUBLE,
    relative_humidity_2m_mean DOUBLE,
    precipitation_sum DOUBLE,
    wind_speed_10m_max DOUBLE,
    wind_gusts_10m_max DOUBLE,
    et0_fao_evapotranspiration DOUBLE,
    risk_index DOUBLE,
    risk_level STRING
)
PARTITIONED BY (year STRING, month STRING, day STRING)
STORED AS PARQUET
LOCATION 's3://fire-risk-pipeline/gold/fire_risk/'
