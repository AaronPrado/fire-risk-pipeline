# Fire Risk Pipeline

[![CI](https://github.com/AaronPrado/fire-risk-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/AaronPrado/fire-risk-pipeline/actions/workflows/ci.yml)

*[Versión en castellano](README.es.md)*

Daily ETL pipeline computing a forest-fire risk index for the seven cities of
Galicia (Spain) from weather data, queryable through a BI dashboard or by asking
questions in plain language to a chatbot.

![Dashboard](docs/dashboard.png)

## Results

- **8,064 historical records** (7 cities × ~1,152 days, 2023-2026) in a medallion
  data lake on S3 with Hive partitioning.
- Index validated against the Xunta de Galicia's official IRDI: seasonal and
  geographic patterns match the wildfires actually recorded in 2024.
- **Text-to-SQL chatbot** running a local LLM: every generated query passes six
  security rules (SELECT-only, table and column whitelists, mandatory partition
  pruning, injected LIMIT), reaching ~83% functional accuracy on a custom
  evaluation dataset.
- **75 tests** across two suites, plus `ruff`, run on every PR. Merging is blocked
  until both jobs are green.

![Chatbot](docs/chatbot.png)

## Stack

Python · Apache Airflow · AWS S3 / Athena / SNS / Glue · Power BI · Docker ·
LangChain + Ollama (qwen2.5-coder:7b) · Gradio · sqlglot · pytest

## Architecture

```
Open-Meteo API → [Airflow DAG] → S3 Bronze → Silver → Gold → SNS Alert
                                                │
                                             Athena
                                             /     \
                                        Power BI   Chatbot (local LLM)
```

## Running it

Local demo on MinIO, no AWS credentials required:

```bash
make demo
```

Requires Docker and Python 3.11+. It starts MinIO, builds its own virtual
environment and writes the partitioned Gold layer to the local bucket.
Console at <http://localhost:9001> (`demo` / `demo12345`); `make demo-down`
tears everything down.

Full AWS pipeline, chatbot, Athena and Power BI setup, IAM permissions, LLM
evaluation and known limitations: **[docs/architecture.md](docs/architecture.md)**
(in Spanish).

Companion project to
[forestfire-cv-detection](https://github.com/AaronPrado/forestfire-cv-detection)
(YOLOv8 + MLflow + FastAPI).

## License

MIT
