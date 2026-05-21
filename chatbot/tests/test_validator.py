import pytest

from chatbot.src.sql.validator import validate_sql, ValidationError


# Querys válidas

def test_query_without_time_filter_is_valid():
    sql = "SELECT location, AVG(risk_index) FROM fire_risk.daily_risk GROUP BY location"
    assert validate_sql(sql) == sql


def test_query_with_point_filter_and_full_partitions_is_valid():
    sql = (
        "SELECT * FROM fire_risk.daily_risk "
        "WHERE time = '2025-08-15' AND year='2025' AND month='08' AND day='15'"
    )
    assert validate_sql(sql) == sql


def test_query_with_between_filter_and_partitions_is_valid():
    sql = (
        "SELECT location, risk_level FROM fire_risk.daily_risk "
        "WHERE time BETWEEN '2025-08-01' AND '2025-08-31' "
        "AND year='2025' AND month='08' AND day='01'"
    )
    assert validate_sql(sql) == sql


def test_query_with_gte_filter_and_partitions_is_valid():
    sql = (
        "SELECT * FROM fire_risk.daily_risk "
        "WHERE time >= '2025-07-01' AND year='2025' AND month='07' AND day='01'"
    )
    assert validate_sql(sql) == sql

    
# Queries rechazadas (falta pruning)

def test_query_with_time_but_missing_year_is_rejected():
    sql = (
        "SELECT * FROM fire_risk.daily_risk "
        "WHERE time = '2025-08-15' AND month='08' AND day='15'"
    )
    with pytest.raises(ValidationError, match="year"):
        validate_sql(sql)


def test_query_with_time_but_missing_month_is_rejected():
    sql = (
        "SELECT * FROM fire_risk.daily_risk "
        "WHERE time = '2025-08-15' AND year='2025' AND day='15'"
    )
    with pytest.raises(ValidationError, match="month"):
        validate_sql(sql)


def test_query_with_time_but_missing_day_is_rejected():
    sql = (
        "SELECT * FROM fire_risk.daily_risk "
        "WHERE time = '2025-08-15' AND year='2025' AND month='08'"
    )
    with pytest.raises(ValidationError, match="day"):
        validate_sql(sql)


def test_query_with_time_but_no_partitions_is_rejected():
    sql = "SELECT * FROM fire_risk.daily_risk WHERE time = '2025-08-15'"
    with pytest.raises(ValidationError):
        validate_sql(sql)


def test_error_message_mentions_missing_partitions():
    sql = "SELECT * FROM fire_risk.daily_risk WHERE time = '2025-08-15'"
    with pytest.raises(ValidationError, match="partición"):
        validate_sql(sql)


# SQL inválido

def test_invalid_sql_syntax_is_rejected():
    sql = "("
    with pytest.raises(ValidationError, match="SQL no válido"):
        validate_sql(sql)
