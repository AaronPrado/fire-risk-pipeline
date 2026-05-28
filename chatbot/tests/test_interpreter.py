from unittest.mock import MagicMock, patch

import pandas as pd

from chatbot.src.llm.interpreter import interpret


def _make_mock_llm(content: str) -> MagicMock:
    """Crea un mock de ChatOllama que devuelve el contenido indicado."""
    mock_llm = MagicMock()
    mock_llm.invoke.return_value.content = content
    return mock_llm


def test_interpret_returns_llm_output_for_valid_df():
    df = pd.DataFrame([{"location": "Vigo", "risk_index": 0.7}])
    headline = "El día con mayor riesgo en Vigo fue el 2024-08-15 con un índice de 0.7."
    with patch("chatbot.src.llm.interpreter.get_llm", return_value=_make_mock_llm(headline)):
        result = interpret("¿Qué día tuvo más riesgo en Vigo?", df)
    assert result == headline


def test_interpret_returns_default_for_empty_df():
    df = pd.DataFrame()
    result = interpret("¿Qué día tuvo más riesgo en Vigo?", df)
    assert result == "La consulta no devolvió resultados."


def test_interpret_truncates_large_df():
    """Verifica que DataFrames grandes incluyen una nota de truncación en el prompt."""
    df = pd.DataFrame({"location": ["Vigo"] * 100, "risk_index": [0.5] * 100})
    mock_llm = _make_mock_llm("Resumen de los datos.")
    with patch("chatbot.src.llm.interpreter.get_llm", return_value=mock_llm):
        interpret("¿Cuál es el riesgo medio?", df)

    called_messages = mock_llm.invoke.call_args[0][0]
    human_content = called_messages[-1].content
    # El prompt debe mencionar el total de filas para que el LLM sepa que ve un subconjunto
    assert "100" in human_content


def test_interpret_returns_empty_on_llm_error():
    df = pd.DataFrame([{"location": "Lugo", "risk_index": 0.3}])
    mock_llm = MagicMock()
    mock_llm.invoke.side_effect = Exception("LLM connection error")
    with patch("chatbot.src.llm.interpreter.get_llm", return_value=mock_llm):
        result = interpret("¿Cuál es el riesgo en Lugo?", df)
    assert result == ""
