import json
from datetime import date
from pathlib import Path

from chatbot.src.schema import catalog

_FEW_SHOT_PATH = Path(__file__).parent.parent.parent / "eval" / "few_shot_examples.jsonl"

_ROLE = """\
Eres un asistente experto en SQL para Athena (Trino dialect).
Tu única tarea es convertir preguntas en lenguaje natural en una consulta SQL válida.

Reglas estrictas:
- Devuelve ÚNICAMENTE la sentencia SQL, sin explicaciones, sin markdown, sin bloques de código.
- Solo puedes consultar la tabla fire_risk.daily_risk.
- Si filtras por 'time', DEBES incluir siempre filtros sobre year, month y day.
- Nunca uses funciones de fecha sobre la columna 'time'; compara siempre con string literals.
- No uses LIMIT superior a 1000.
- No añadas filtros sobre 'time', 'year', 'month' o 'day' si la pregunta no menciona fechas.
- Cuando la pregunta filtre por una condición sobre una columna numérica (ej: 'temperatura > 35', 'humedad < 40', 'viento > 80'), incluye esa columna en el SELECT para que el valor sea visible.
- Si filtras por 'risk_level', incluye también 'risk_index' en el SELECT.
- Excepción a las dos reglas anteriores: si la pregunta pide una agregación (COUNT, SUM, AVG, MAX, MIN), devuelve solo el agregado sin añadir esas columnas.\
"""


def _serialize_catalog() -> str:
    """Serializa el catálogo de la tabla en formato legible."""
    table = catalog.TABLE

    lines = [
        f"Tabla: {table['name']}",
        "",
        "Columnas:",
    ]
    for col in table["columns"]:
        lines.append(f"  - {col['name']} ({col['type']}): {col['description']}")

    lines += ["", "Particiones (obligatorias para pruning):"]
    for part in table["partitions"]:
        lines.append(f"  - {part['name']} ({part['type']}): {part['description']}")

    lines += ["", "Notas importantes:"]
    for note in table["notes"]:
        lines.append(f"  * {note}")

    return "\n".join(lines)


def _build_date_context() -> str:
    """Inyecta la fecha actual para que el LLM resuelva referencias relativas."""
    today = date.today().isoformat()
    return (
        f"Fecha actual: {today}.\n"
        f"Cuando la pregunta use referencias relativas (hoy, ayer, este mes, año pasado, "
        f"último trimestre, etc.), resuélvelas usando esta fecha y convierte el resultado a "
        f"string literals. Nunca uses funciones de fecha como CURRENT_DATE o YEAR()."
    )


def _load_few_shot_examples() -> str:
    """Carga los ejemplos de few-shot desde el archivo JSONL."""
    lines = ["Ejemplos:"]
    for raw in _FEW_SHOT_PATH.read_text(encoding="utf-8").splitlines():
        raw = raw.strip()
        if not raw:
            continue
        example = json.loads(raw)
        lines.append(f"\nPregunta: {example['question']}")
        lines.append(f"SQL: {example['sql']}")
    return "\n".join(lines)


def build_system_prompt() -> str:
    """Construye el system prompt completo para el generador de SQL."""
    sections = [
        _ROLE,
        "---",
        _build_date_context(),
        "---",
        _serialize_catalog(),
        "---",
        _load_few_shot_examples(),
    ]
    return "\n\n".join(sections)
