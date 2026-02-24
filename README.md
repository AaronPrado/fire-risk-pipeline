# Fire Risk Pipeline

Pipeline ETL construido con Apache Airflow que extrae datos meteorológicos diarios de la API Open-Meteo, calcula el riesgo de incendio forestal para ciudades gallegas y almacena los resultados en AWS S3 siguiendo una arquitectura Bronze/Silver/Gold.

Proyecto complementario a [forestfire-cv-detection](https://github.com/AaronPrado/forestfire-cv-detection).

## Stack Tecnológico

- **Orquestación:** Apache Airflow 2.10.4 (LocalExecutor)
- **Almacenamiento:** AWS S3 (capas Bronze / Silver / Gold)
- **Fuente de datos:** [Open-Meteo API](https://open-meteo.com/) (gratuita, sin API key)
- **Contenedores:** Docker + Docker Compose
- **Lenguaje:** Python 3.11

## Estructura del Proyecto

```
fire-risk-pipeline/
├── configs/
│   └── config.yaml            # Configuración centralizada (localidades, variables, pesos, AWS)
├── dags/
│   └── fire_risk_daily.py     # DAG principal de Airflow
├── docker/
│   ├── docker-compose.yml     # Servicios Airflow + PostgreSQL
│   └── Dockerfile             # Imagen custom de Airflow
├── src/
│   ├── extractors/
│   │   └── open_meteo.py      # Cliente de la API Open-Meteo
│   ├── transformers/
│   │   ├── validators.py      # Validación y limpieza de datos
│   │   └── risk_calculator.py # Cálculo de índice de riesgo
│   └── utils/
│       └── config.py          # Cargador de configuración YAML
├── tests/
├── .env.example               # Plantilla de variables de entorno
└── requirements.txt           # Dependencias Python
```

## Cobertura

7 ciudades gallegas: A Coruña, Ferrol, Lugo, Ourense, Santiago de Compostela, Pontevedra, Vigo.

## Variables Meteorológicas

| Variable | Relevancia |
|---|---|
| `temperature_2m_max` | Temperaturas altas secan la vegetación |
| `temperature_2m_min` | Indicador de amplitud térmica |
| `relative_humidity_2m_mean` | Humedad baja aumenta el riesgo |
| `precipitation_sum` | Falta de lluvia aumenta el riesgo |
| `wind_speed_10m_max` | El viento propaga el fuego |
| `wind_gusts_10m_max` | Rachas extremas = riesgo extremo |
| `et0_fao_evapotranspiration` | Pérdida de humedad del suelo/vegetación |

## Cálculo de Riesgo

El índice de riesgo se calcula mediante un FWI (Fire Weather Index) simplificado:

1. **Normalización** de cada variable a escala 0-1
2. **Inversión** de humedad y precipitación (más = menos riesgo)
3. **Suma ponderada** con pesos configurables por variable
4. **Factor estacional** basado en el patrón bimodal de incendios en Galicia (picos en febrero-marzo y agosto-septiembre)
5. **Clasificación** en 5 niveles: low, moderate, high, very_high, extreme

## Pipeline (DAG)

```
extract_weather → validate_weather → calculate_risk
    (Bronze)          (Silver)           (Gold)
```

| Tarea | Entrada | Salida |
|---|---|---|
| `extract_weather` | Open-Meteo API | `bronze/weather/{fecha}/raw.json` |
| `validate_weather` | JSON raw | `silver/weather/{fecha}/clean.parquet` |
| `calculate_risk` | Parquet limpio | `gold/fire_risk/{fecha}/risk.parquet` |

## Instalación

1. Clona el repositorio
2. Copia `.env.example` a `.env` y rellena tus credenciales AWS
3. Arranca Airflow:
   ```bash
   cd docker
   docker compose --env-file ../.env up --build
   ```
4. Accede a la UI en `http://localhost:8080` (admin/admin)

## Progreso

- [x] Fase 1: Setup de Airflow con Docker (LocalExecutor + PostgreSQL + S3)
- [x] Fase 2: Extracción de datos meteorológicos (capa Bronze)
- [x] Fase 3: Validación de datos (capa Silver)
- [x] Fase 4: Cálculo de riesgo de incendio (capa Gold)
- [ ] Fase 5: Reporting y alertas
- [ ] Fase 6: Tests y backfill
