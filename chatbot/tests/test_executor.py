import pandas as pd
from unittest.mock import MagicMock, patch

from chatbot.src.sql.executor import run_query


def make_mock_cursor(df: pd.DataFrame) -> MagicMock:
    mock_cursor = MagicMock()
    mock_cursor.execute.return_value.as_pandas.return_value = df

    mock_connection = MagicMock()
    mock_connection.cursor.return_value = mock_cursor

    return mock_connection


@patch("chatbot.src.sql.executor.connect")
def test_run_query_returns_list_of_dicts(mock_connect: MagicMock) -> None:
    df = pd.DataFrame([{"location": "Vigo", "risk_index": 0.7}])
    mock_connect.return_value = make_mock_cursor(df)

    result = run_query("SELECT location, risk_index FROM fire_risk.daily_risk LIMIT 1")

    assert result == [{"location": "Vigo", "risk_index": 0.7}]


@patch("chatbot.src.sql.executor.connect")
def test_run_query_empty_result(mock_connect: MagicMock) -> None:
    df = pd.DataFrame(columns=["location", "risk_index"])
    mock_connect.return_value = make_mock_cursor(df)

    result = run_query("SELECT location, risk_index FROM fire_risk.daily_risk WHERE 1=0")

    assert result == []