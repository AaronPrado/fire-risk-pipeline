TABLE: dict = {
    "name": "fire_risk.daily_risk",
    "columns": [
        {"name": "time",                       "type": "STRING",  "description": "Fecha de la medición. Formato YYYY-MM-DD. Comparar siempre como string literal, nunca con funciones de fecha."},
        {"name": "location",                   "type": "STRING",  "description": "Ciudad gallega. Valores válidos: 'A Coruña', 'Ferrol', 'Lugo', 'Ourense', 'Pontevedra', 'Santiago de Compostela', 'Vigo'."},
        {"name": "temperature_2m_max",         "type": "DOUBLE",  "description": "Temperatura máxima diaria a 2 metros (°C)."},
        {"name": "temperature_2m_min",         "type": "DOUBLE",  "description": "Temperatura mínima diaria a 2 metros (°C)."},
        {"name": "relative_humidity_2m_mean",  "type": "DOUBLE",  "description": "Humedad relativa media diaria (%). A mayor humedad, menor riesgo."},
        {"name": "precipitation_sum",          "type": "DOUBLE",  "description": "Precipitación total diaria (mm). A mayor precipitación, menor riesgo."},
        {"name": "wind_speed_10m_max",         "type": "DOUBLE",  "description": "Velocidad máxima del viento a 10 metros (km/h)."},
        {"name": "wind_gusts_10m_max",         "type": "DOUBLE",  "description": "Velocidad máxima de las ráfagas a 10 metros (km/h)."},
        {"name": "et0_fao_evapotranspiration", "type": "DOUBLE",  "description": "Evapotranspiración de referencia FAO (mm). Mide la sequedad del suelo."},
        {"name": "risk_index",                 "type": "DOUBLE",  "description": "Índice de riesgo de incendio normalizado entre 0 y 1. Calculado como suma ponderada de variables meteorológicas con factor estacional."},
        {"name": "risk_level",                 "type": "STRING",  "description": "Nivel de riesgo categórico. Valores en inglés: 'low', 'moderate', 'high', 'very_high', 'extreme'."},
    ],
    "partitions": [
        {"name": "year",  "type": "STRING", "description": "Año de la medición. STRING con 4 dígitos, ej: '2025'."},
        {"name": "month", "type": "STRING", "description": "Mes de la medición. STRING con cero a la izquierda, ej: '08' no '8'."},
        {"name": "day",   "type": "STRING", "description": "Día de la medición. STRING con cero a la izquierda, ej: '05' no '5'."},
    ],
    "notes": [
        "Filtros sobre 'time' DEBEN incluir siempre filtros paralelos sobre las particiones year, month y day para evitar full scan.",
        "Ejemplo correcto: WHERE time BETWEEN '2025-08-01' AND '2025-08-31' AND year='2025' AND month='08'.",
        "risk_level está en inglés. Mapping: bajo=low, moderado=moderate, alto=high, muy alto=very_high, extremo=extreme.",
        "location distingue mayúsculas y tildes: usar exactamente 'A Coruña', 'Santiago de Compostela', etc.",
    ],
    "risk_level_mapping": {
        "bajo": "low", "baja": "low",
        "moderado": "moderate", "moderada": "moderate",
        "alto": "high", "alta": "high",
        "muy alto": "very_high", "muy alta": "very_high",
        "extremo": "extreme", "extrema": "extreme",
    },
}