import pandas as pd
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

from chatbot.src import config


def run_query(sql: str) -> list[dict]:
    """
    Ejecuta una query SQL contra Athena y devuelve los resultados como lista de dicts.

    Args:
        sql: Query SQL válida

    Returns:
        Lista de dicts con una entrada por fila, o [] si no hay resultados.

    Raises:
        pyathena.error.OperationalError: Si Athena rechaza la query en ejecución.
    """
    cursor = connect(
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
        region_name=config.AWS_DEFAULT_REGION,
        s3_staging_dir=config.ATHENA_RESULTS_BUCKET,
        schema_name=config.ATHENA_DATABASE,
        cursor_class=PandasCursor,
    ).cursor()

    df: pd.DataFrame = cursor.execute(sql).as_pandas()
    return df.to_dict(orient="records")
