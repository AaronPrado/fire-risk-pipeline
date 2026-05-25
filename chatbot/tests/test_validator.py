import pytest

from chatbot.src.sql.validator import validate_sql, ValidationError


# Querys válidas

def test_query_without_time_filter_is_valid():
    sql = "SELECT location, AVG(risk_index) FROM fire_risk.daily_risk GROUP BY location LIMIT 100"
    assert validate_sql(sql) == sql


def test_query_with_point_filter_and_full_partitions_is_valid():
    sql = (
        "SELECT * FROM fire_risk.daily_risk "
        "WHERE time = '2025-08-15' AND year='2025' AND month='08' AND day='15' LIMIT 10"
    )
    assert validate_sql(sql) == sql


def test_query_with_between_filter_and_partitions_is_valid():
    sql = (
        "SELECT location, risk_level FROM fire_risk.daily_risk "
        "WHERE time BETWEEN '2025-08-01' AND '2025-08-31' "
        "AND year='2025' AND month='08' AND day='01' LIMIT 50"
    )
    assert validate_sql(sql) == sql


def test_query_with_gte_filter_and_partitions_is_valid():
    sql = (
        "SELECT * FROM fire_risk.daily_risk "
        "WHERE time >= '2025-07-01' AND year='2025' AND month='07' AND day='01' LIMIT 100"
    )
    assert validate_sql(sql) == sql


def test_query_with_between_without_day_is_valid():
    sql = (
        "SELECT * FROM fire_risk.daily_risk "
        "WHERE time BETWEEN '2025-08-01' AND '2025-08-31' "
        "AND year='2025' AND month='08' LIMIT 100"
    )
    assert validate_sql(sql) == sql


def test_query_with_gte_without_day_is_valid():
    sql = (
        "SELECT * FROM fire_risk.daily_risk "
        "WHERE time >= '2025-07-01' AND year='2025' AND month='07' LIMIT 100"
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


# LIMIT injection

def test_limit_is_injected_when_missing():
    sql = "SELECT location FROM fire_risk.daily_risk"
    assert validate_sql(sql) == sql + " LIMIT 1000"


def test_limit_above_max_is_replaced():
    sql = "SELECT location FROM fire_risk.daily_risk LIMIT 99999"
    assert validate_sql(sql) == "SELECT location FROM fire_risk.daily_risk LIMIT 1000"


# SQL inválido

def test_invalid_sql_syntax_is_rejected():
    sql = "("
    with pytest.raises(ValidationError, match="SQL no válido"):
        validate_sql(sql)


# Prompt injection

def test_injection_multiple_statements_is_rejected():
    sql = "SELECT location FROM fire_risk.daily_risk LIMIT 10; DROP TABLE fire_risk.daily_risk"
    with pytest.raises(ValidationError, match="sentencias"):
        validate_sql(sql)


def test_injection_union_unauthorized_table_is_rejected():
    sql = (
        "SELECT location FROM fire_risk.daily_risk "
        "UNION SELECT table_name FROM information_schema.tables"
    )
    with pytest.raises(ValidationError, match="SELECT"):
        validate_sql(sql)


def test_injection_subquery_unauthorized_table_is_rejected():
    sql = "SELECT * FROM (SELECT * FROM users) t"
    with pytest.raises(ValidationError, match="Tabla no autorizada"):
        validate_sql(sql)


def test_injection_insert_disguised_is_rejected():
    sql = "INSERT INTO users SELECT * FROM fire_risk.daily_risk LIMIT 10"
    with pytest.raises(ValidationError, match="SELECT"):
        validate_sql(sql)


def test_injection_system_column_is_rejected():
    sql = "SELECT password FROM fire_risk.daily_risk LIMIT 10"
    with pytest.raises(ValidationError, match="Columna no autorizada"):
        validate_sql(sql)


def test_alias_in_order_by_is_not_rejected():
    sql = (
        "SELECT risk_level, COUNT(*) AS days_count FROM fire_risk.daily_risk "
        "WHERE year = '2024' GROUP BY risk_level ORDER BY days_count DESC LIMIT 1000"
    )
    assert validate_sql(sql) == sql
