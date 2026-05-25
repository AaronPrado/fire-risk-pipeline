import sqlglot
import sqlglot.expressions as exp
import re

from chatbot.src.schema import catalog

MAX_LIMIT = 1000

class ValidationError(Exception):
    """Se lanza cuando una query SQL no pasa la validación"""
    pass


def validate_sql(sql: str) -> str:
    """
    Valida la query SQL antes de ejecutarla contra Athena.

    Reglas:
    1. Query válida sintácticamente.
    2. Solo se permiten statements SELECT.
    3. Solo se permite la tabla autorizada (fire_risk.daily_risk).
    4. Si hay filtro sobre 'time':
       - Filtro puntual (time = 'X'): exigir year, month y day.
       - Filtro de rango (BETWEEN, >=, <=, etc.): exigir year y month.
    5. Solo se permiten columnas autorizadas.
    6. Se inyecta LIMIT MAX_LIMIT si no existe, es mayor a MAX_LIMIT o ilegible.

    Returns:
        Query original si es válida.

    Raises:
        ValidationError: Si la query no cumple alguna regla.
    """
    parsed = _parse(sql)
    _check_select_only(parsed)
    _check_table_whitelist(parsed)
    _check_column_whitelist(parsed)
    _check_partition_pruning(parsed)
    return _inject_limit(parsed, sql)


def _parse(sql: str) -> exp.Expression:
    """Parsea la query. ValidationError si la sintaxis es incorrecta o hay múltiples sentencias."""
    try:
        statements = sqlglot.parse(sql, dialect="trino")
    except sqlglot.errors.ParseError as e:
        raise ValidationError(f"SQL no válido: {e}") from e

    if len(statements) > 1:
        raise ValidationError(
            f"Solo se permite una sentencia por query. "
            f"Se detectaron {len(statements)} sentencias."
        )

    return statements[0]


def _has_column_filter(where: exp.Where | None, column_name: str) -> bool:
    """Devuelve True si la cláusula WHERE contiene un filtro sobre la columna indicada."""
    if where is None:
        return False
    return any(
        isinstance(node, exp.Column) and node.name.lower() == column_name.lower()
        for node in where.walk()
    )


_RANGE_OPERATORS = (exp.Between, exp.GT, exp.GTE, exp.LT, exp.LTE, exp.NEQ)


def _time_filter_kind(where: exp.Where | None) -> str | None:
    """Devuelve 'point' si time se filtra con =, 'range' si es BETWEEN/>=/<=/etc, None si no."""
    if where is None:
        return None
    for node in where.walk():
        if isinstance(node, exp.Column) and node.name.lower() == "time":
            parent = node.parent
            if isinstance(parent, exp.EQ):
                return "point"
            if isinstance(parent, _RANGE_OPERATORS):
                return "range"
    return None


def _check_partition_pruning(parsed: exp.Expression) -> None:
    """
    Lanza ValidationError si hay filtro temporal sin los filtros de partición obligatorios.

    Filtro puntual (time = 'X'): exigir year, month y day.
    Filtro de rango (BETWEEN, >=, <=, etc.): exigir year y month (day opcional).
    """
    where = parsed.find(exp.Where)
    kind = _time_filter_kind(where)

    if kind is None:
        return

    required = ["year", "month", "day"] if kind == "point" else ["year", "month"]
    missing = [col for col in required if not _has_column_filter(where, col)]

    if missing:
        raise ValidationError(
            f"Query rechazada: filtro sobre 'time' detectado pero faltan filtros "
            f"de partición: {missing}. "
            f"Añade filtros explícitos para evitar full scan. "
            f"Ejemplo: AND year='2025' AND month='08'."
        )


def _check_select_only(parsed: exp.Expression) -> None:
    """Rechaza cualquier statement que no sea SELECT."""
    if not isinstance(parsed, exp.Select):
        raise ValidationError(
            f"Solo se permiten consultas SELECT. "
            f"Statement recibido: {type(parsed).__name__}."
        )


def _check_table_whitelist(parsed: exp.Expression) -> None:
    """Rechaza queries que referencien tablas no autorizadas."""
    allowed = catalog.TABLE["name"]
    allowed_short = allowed.split(".")[-1]

    for table in parsed.find_all(exp.Table):
        full_name = f"{table.db}.{table.name}" if table.db else table.name
        if full_name not in (allowed, allowed_short):
            raise ValidationError(
                f"Tabla no autorizada: '{full_name}'. "
                f"Solo se permite: '{allowed}'."
            )


def _check_column_whitelist(parsed: exp.Expression) -> None:
    """Rechaza queries que referencien columnas no autorizadas."""
    valid_columns = (
        {c["name"] for c in catalog.TABLE["columns"]}
        | {p["name"] for p in catalog.TABLE["partitions"]}
    )
    select_aliases = {alias.alias.lower() for alias in parsed.find_all(exp.Alias) if alias.alias}

    for col in parsed.find_all(exp.Column):
        if col.name.lower() not in valid_columns and col.name.lower() not in select_aliases:
            raise ValidationError(
                f"Columna no autorizada: '{col.name}'. "
                f"Columnas permitidas: {sorted(valid_columns)}."
            )


def _inject_limit(parsed: exp.Expression, sql: str) -> str:
    """
    Aplica el límite máximo de filas.

    - Sin LIMIT: inyecta LIMIT MAX_LIMIT.
    - LIMIT > MAX_LIMIT o ilegible: reemplaza con MAX_LIMIT.
    - LIMIT <= MAX_LIMIT: devuelve el SQL intacto.
    """
    limit_node = parsed.find(exp.Limit)

    if limit_node is None:
        return sql.rstrip().rstrip(";") + f" LIMIT {MAX_LIMIT}"

    try:
        limit_value = int(limit_node.expression.this)
    except (ValueError, AttributeError):
        limit_value = MAX_LIMIT + 1

    if limit_value > MAX_LIMIT:
        return re.sub(r"LIMIT\s+\d+", f"LIMIT {MAX_LIMIT}", sql, flags=re.IGNORECASE)

    return sql