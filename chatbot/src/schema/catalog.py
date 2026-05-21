from pathlib import Path

import sqlglot
import sqlglot.expressions as exp
import yaml

_REPO_ROOT = Path(__file__).parent.parent.parent.parent
_DDL_PATH = _REPO_ROOT / "sql" / "create_table.sql"
_CONFIG_PATH = _REPO_ROOT / "configs" / "config.yaml"

_DESCRIPTIONS: dict[str, str] = {
    "time": "Fecha de la medición. Formato YYYY-MM-DD. Comparar siempre como string literal, nunca con funciones de fecha.",
    "location": "Ciudad gallega. Valores válidos: {cities}.",
    "temperature_2m_max": "Temperatura máxima diaria a 2 metros (°C).",
    "temperature_2m_min": "Temperatura mínima diaria a 2 metros (°C).",
    "relative_humidity_2m_mean": "Humedad relativa media diaria (%). A mayor humedad, menor riesgo.",
    "precipitation_sum": "Precipitación total diaria (mm). A mayor precipitación, menor riesgo.",
    "wind_speed_10m_max": "Velocidad máxima del viento a 10 metros (km/h).",
    "wind_gusts_10m_max": "Velocidad máxima de las ráfagas a 10 metros (km/h).",
    "et0_fao_evapotranspiration": "Evapotranspiración de referencia FAO (mm). Mide la sequedad del suelo.",
    "risk_index": "Índice de riesgo de incendio normalizado entre 0 y 1.",
    "risk_level": "Nivel de riesgo categórico. Valores en inglés: 'low', 'moderate', 'high', 'very_high', 'extreme'.",
}

_PARTITION_DESCRIPTIONS: dict[str, str] = {
    "year": "Año de la medición. STRING con 4 dígitos, ej: '2025'.",
    "month": "Mes de la medición. STRING con cero a la izquierda, ej: '08' no '8'.",
    "day": "Día de la medición. STRING con cero a la izquierda, ej: '05' no '5'.",
}

_NOTES: list[str] = [
    "Filtros sobre 'time' DEBEN incluir siempre filtros paralelos sobre las particiones year, month y day para evitar full scan.",
    "Ejemplo correcto: WHERE time BETWEEN '2025-08-01' AND '2025-08-31' AND year='2025' AND month='08'.",
    "risk_level está en inglés. Mapping: bajo=low, moderado=moderate, alto=high, muy alto=very_high, extremo=extreme.",
    "location distingue mayúsculas y tildes: usar exactamente 'A Coruña', 'Santiago de Compostela', etc.",
]

_RISK_LEVEL_MAPPING: dict[str, str] = {
    "bajo": "low", "baja": "low",
    "moderado": "moderate", "moderada": "moderate",
    "alto": "high", "alta": "high",
    "muy alto": "very_high", "muy alta": "very_high",
    "extremo": "extreme", "extrema": "extreme",
}


def _parse_ddl(ddl_path: Path) -> tuple[list[dict], list[dict]]:
    """Parsea el DDL y devuelve (columns, partitions) con name, type y description."""
    parsed = sqlglot.parse_one(ddl_path.read_text(), dialect="hive")

    columns = [
        {
            "name": col.name,
            "type": col.args["kind"].sql(dialect="hive"),
            "description": _DESCRIPTIONS.get(col.name, ""),
        }
        for col in parsed.find(exp.Schema).expressions
        if isinstance(col, exp.ColumnDef)
    ]

    partitions = [
        {
            "name": col.name,
            "type": col.args["kind"].sql(dialect="hive"),
            "description": _PARTITION_DESCRIPTIONS.get(col.name, ""),
        }
        for col in parsed.find(exp.PartitionedByProperty).find_all(exp.ColumnDef)
    ]

    return columns, partitions


def _load_cities(config_path: Path) -> str:
    """Lee los nombres de ciudades de config.yaml y devuelve un string separado por comas."""
    with config_path.open() as f:
        config = yaml.safe_load(f)
    return ", ".join(loc["name"] for loc in config["locations"])


def _build_table(ddl_path: Path, config_path: Path) -> dict:
    """Construye el TABLE dict combinando DDL, config.yaml y metadata."""
    columns, partitions = _parse_ddl(ddl_path)
    cities = _load_cities(config_path)

    for col in columns:
        if col["name"] == "location":
            col["description"] = col["description"].format(cities=cities)

    return {
        "name": "fire_risk.daily_risk",
        "columns": columns,
        "partitions": partitions,
        "notes": _NOTES,
        "risk_level_mapping": _RISK_LEVEL_MAPPING,
    }


TABLE: dict = _build_table(_DDL_PATH, _CONFIG_PATH)
