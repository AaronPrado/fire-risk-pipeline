import re

from langchain_core.messages import HumanMessage, SystemMessage

from chatbot.src.llm.client import get_llm
from chatbot.src.llm.prompts import build_system_prompt
from chatbot.src.sql.validator import validate_sql


def _clean_sql(text: str) -> str:
    """Elimina bloques markdown y punto y coma final del output del LLM."""
    text = re.sub(r"```(?:sql)?\s*", "", text, flags=re.IGNORECASE)
    text = text.replace("```", "")
    return text.strip().rstrip(";").strip()


def generate_sql(question: str) -> str:
    """Convierte una pregunta en lenguaje natural en SQL.

    Args:
        question: Pregunta del usuario en lenguaje natural.

    Returns:
        SQL validado y listo para ejecutar en Athena.

    Raises:
        ValidationError: Si el SQL no pasa las reglas de seguridad.
    """
    system_prompt = build_system_prompt()
    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=question),
    ]

    response = get_llm().invoke(messages)
    raw_sql = _clean_sql(response.content)

    return validate_sql(raw_sql)
