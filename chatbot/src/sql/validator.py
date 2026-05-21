import sqlglot
import sqlglot.expressions as exp

from chatbot.src.schema import catalog


class ValidationError(Exception):
    """Se lanza cuando una query SQL no pasa la validación"""
    pass


def validate_sql(sql: str) -> str:
    """
    Valida la query SQL antes de ejecutarla contra Athena.

    Reglas :
    1. Query válida sintácticamente.
    2. Si hay filtro sobre 'time', deben existir filtros sobre year, month y day
       para evitar scan sobre las particiones de S3.

    Returns:
        Query original si es válida.

    Raises:
        ValidationError: Si la query no cumple alguna regla.
    """
    parsed = _parse(sql)
    _check_partition_pruning(parsed)
    return sql


def _parse(sql: str) -> exp.Expression:
    """Parsea la query. Lanza ValidationError si la sintaxis es incorrecta."""
    try:
        return sqlglot.parse_one(sql, dialect="trino")
    except sqlglot.errors.ParseError as e:
        raise ValidationError(f"SQL no válido: {e}") from e


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
