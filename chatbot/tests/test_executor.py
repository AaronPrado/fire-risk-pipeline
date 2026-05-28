from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from chatbot.src.athena.executor import run_query


def _make_mock_connect(df: pd.DataFrame) -> MagicMock:
    """Crea un mock de pyathena.connect que devuelve el DataFrame indicado."""
    mock_cursor = MagicMock()
    mock_cursor.execute.return_value.as_pandas.return_value = df

    mock_connection = MagicMock()
    mock_connection.cursor.return_value = mock_cursor

    return mock_connection


@patch("chatbot.src.athena.executor.connect")
def test_run_query_returns_dataframe(mock_connect: MagicMock) -> None:
    df = pd.DataFrame([{"location": "Vigo", "risk_index": 0.7}])
    mock_connect.return_value = _make_mock_connect(df)

    result = run_query("SELECT location, risk_index FROM fire_risk.daily_risk LIMIT 1")

    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["location", "risk_index"]
    assert result.iloc[0]["location"] == "Vigo"


@patch("chatbot.src.athena.executor.connect")
def test_run_query_empty_result_returns_empty_dataframe(mock_connect: MagicMock) -> None:
    df = pd.DataFrame(columns=["location", "risk_index"])
    mock_connect.return_value = _make_mock_connect(df)

    result = run_query("SELECT location, risk_index FROM fire_risk.daily_risk WHERE 1=0")

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0


@patch("chatbot.src.athena.executor.connect")
def test_run_query_propagates_exception(mock_connect: MagicMock) -> None:
    mock_connect.side_effect = Exception("Connection error")

    with pytest.raises(Exception, match="Connection error"):
        run_query("SELECT location FROM fire_risk.daily_risk LIMIT 1")
