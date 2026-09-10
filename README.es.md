# Fire Risk Pipeline

[![CI](https://github.com/AaronPrado/fire-risk-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/AaronPrado/fire-risk-pipeline/actions/workflows/ci.yml)

Pipeline ETL que calcula a diario el índice de riesgo de incendio forestal en las
siete ciudades gallegas a partir de datos meteorológicos, y permite consultarlo
en un dashboard o preguntándole en castellano a un chatbot.

![Dashboard](docs/dashboard.png)

## Resultados

- **8.064 registros** históricos (7 ciudades × ~1.152 días, 2023-2026) en un data
  lake medallion sobre S3 con particionado Hive.
- Índice validado contra el [IRDI de la Xunta de Galicia](https://mediorural.xunta.gal/es/temas/defensa-monte/irdi):
  los patrones estacionales y geográficos coinciden con los incendios reales de 2024.
- **Chatbot text-to-SQL** con LLM local: valida cada query con 6 reglas de seguridad
  (solo SELECT, whitelist de tablas y columnas, partition pruning obligatorio, LIMIT
  inyectado) y alcanza ~83% de acierto funcional sobre un dataset de evaluación propio.
- **75 tests** en dos suites, más `ruff`, ejecutados en cada PR. El merge queda
  bloqueado hasta que ambos jobs estén en verde.

![Chatbot](docs/chatbot.png)

## Stack

Python · Apache Airflow · AWS S3 / Athena / SNS / Glue · Power BI · Docker ·
LangChain + Ollama (qwen2.5-coder:7b) · Gradio · sqlglot · pytest

## Arquitectura

```
Open-Meteo API → [Airflow DAG] → S3 Bronze → Silver → Gold → SNS Alert
                                                │
                                             Athena
                                             /     \
                                        Power BI   Chatbot (LLM local)
```

## Ejecución

Demo local sobre MinIO, sin credenciales de AWS:

```bash
make demo
```

Requiere Docker y Python 3.11+. Levanta MinIO, se monta su propio entorno
virtual y escribe la capa Gold particionada en el bucket local. Consola en
<http://localhost:9001> (`demo` / `demo12345`); `make demo-down` para limpiar.

Pipeline completo contra AWS, chatbot, configuración de Athena y Power BI,
permisos IAM, evaluación del LLM y limitaciones conocidas:
**[docs/architecture.md](docs/architecture.md)**.

Proyecto complementario a
[forestfire-cv-detection](https://github.com/AaronPrado/forestfire-cv-detection)
(YOLOv8 + MLflow + FastAPI).

## Licencia

MIT
