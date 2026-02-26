-- Discover and register all Hive partitions in the Glue metastore
-- Run this after backfill or when new partitions are added to S3
MSCK REPAIR TABLE fire_risk.daily_risk;
