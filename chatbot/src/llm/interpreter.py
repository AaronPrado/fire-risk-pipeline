import pandas as pd
from langchain_core.messages import HumanMessage, SystemMessage

from chatbot.src.llm.client import get_llm
from chatbot.src.llm.prompts import _INTERPRETATION_ROLE, build_interpretation_prompt

_MAX_ROWS = 20
_DEFAULT_EMPTY = "La consulta no devolvió resultados."


def interpret(question: str, df: pd.DataFrame) -> str:
    """Resume los resultados de una consulta en lenguaje natural.

    Args:
        question: Pregunta original del usuario.
        df: DataFrame con los resultados de la consulta Athena.

    Returns:
        Respuesta en lenguaje natural, o cadena vacía si el LLM falla.
    """
    if df.empty:
        return _DEFAULT_EMPTY

    total_rows = len(df)
    display_df = df.head(_MAX_ROWS)
    data_str = display_df.to_string(index=False)
    data_note = f"(mostrando {_MAX_ROWS} de {total_rows} filas)" if total_rows > _MAX_ROWS else ""

    human_content = build_interpretation_prompt(question, data_str, data_note)
    messages = [
        SystemMessage(content=_INTERPRETATION_ROLE),
        HumanMessage(content=human_content),
    ]

    try:
        response = get_llm().invoke(messages)
        return response.content.strip()
    except Exception:
        return ""
