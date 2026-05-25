from unittest.mock import MagicMock, patch

import pytest
from langchain_core.messages import HumanMessage, SystemMessage

from chatbot.src.llm.generator import generate_sql
from chatbot.src.sql.validator import ValidationError


def _make_mock_llm(sql: str) -> MagicMock:
    """Crea un mock de ChatOllama que devuelve el SQL indicado."""
    mock_llm = MagicMock()
    mock_llm.invoke.return_value.content = sql
    return mock_llm


def test_generate_sql_returns_valid_sql():
    sql = "SELECT location, AVG(risk_index) FROM fire_risk.daily_risk GROUP BY location LIMIT 10"
    with patch("chatbot.src.llm.generator.get_llm", return_value=_make_mock_llm(sql)):
        result = generate_sql("¿Cuál es el riesgo medio por ciudad?")
    assert result == sql


def test_generate_sql_strips_markdown_fences():
    raw = "```sql\nSELECT location FROM fire_risk.daily_risk LIMIT 10\n```"
    expected = "SELECT location FROM fire_risk.daily_risk LIMIT 10"
    with patch("chatbot.src.llm.generator.get_llm", return_value=_make_mock_llm(raw)):
        result = generate_sql("¿Qué ciudades hay?")
    assert result == expected


def test_generate_sql_raises_on_invalid_sql():
    with patch("chatbot.src.llm.generator.get_llm", return_value=_make_mock_llm("(")):
        with pytest.raises(ValidationError, match="SQL no válido"):
            generate_sql("pregunta cualquiera")


def test_generate_sql_passes_correct_messages_to_llm():
    sql = "SELECT COUNT(*) FROM fire_risk.daily_risk LIMIT 10"
    mock_llm = _make_mock_llm(sql)
    question = "¿Cuántos registros hay en total?"

    with patch("chatbot.src.llm.generator.get_llm", return_value=mock_llm):
        generate_sql(question)

    called_messages = mock_llm.invoke.call_args[0][0]
    assert isinstance(called_messages[0], SystemMessage)
    assert isinstance(called_messages[1], HumanMessage)
    assert called_messages[1].content == question
