FROM apache/airflow:2.10.4-python3.11

USER root
RUN apt-get update && apt-get install -y libxml2-dev libxslt-dev && rm -rf /var/lib/apt/lists/*
USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
