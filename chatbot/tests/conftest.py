import os

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test_access_key_id")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test_secret_access_key")
os.environ.setdefault("AWS_DEFAULT_REGION", "eu-west-1")
os.environ.setdefault("ATHENA_DATABASE", "fire_risk")
os.environ.setdefault("ATHENA_TABLE", "daily_risk")
os.environ.setdefault("ATHENA_RESULTS_BUCKET", "s3://fire-risk-pipeline/athena-results/")
