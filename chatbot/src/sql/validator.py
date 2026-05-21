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
    4. Si hay filtro sobre 'time', deben existir filtros sobre year, month y day
       para evitar scan sobre las particiones de S3.
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


def _check_partition_pruning(parsed: exp.Expression) -> None:
    """
    Lanza ValidationError si hay filtro temporal sin los filtros de partición obligatorios.

    Cuando se filtra por 'time', Athena haría full scan sobre todas las particiones
    si no se añaden filtros explícitos sobre year, month y day.
    """
    where = parsed.find(exp.Where)

    if not _has_column_filter(where, "time"):
        return  # Sin filtro temporal, no aplica la regla

    partition_cols = [p["name"] for p in catalog.TABLE["partitions"]]
    missing = [col for col in partition_cols if not _has_column_filter(where, col)]

    if missing:
        raise ValidationError(
            f"Query rechazada: filtro sobre 'time' detectado pero faltan filtros "
            f"de partición: {missing}. "
            f"Añade filtros explícitos para evitar full scan. "
            f"Ejemplo: AND year='2025' AND month='08' AND day='15'."
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

    for col in parsed.find_all(exp.Column):
        if col.name.lower() not in valid_columns:
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