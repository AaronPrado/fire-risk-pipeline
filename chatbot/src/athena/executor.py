import pandas as pd
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

from chatbot.src import config


def run_query(sql: str) -> pd.DataFrame:
    """Ejecuta una query SQL contra Athena y devuelve los resultados como DataFrame.

    Args:
        sql: Query SQL validada.

    Returns:
        DataFrame con los resultados.

    Raises:
        Exception: Error de conexión o ejecución sube al caller.
    """
    cursor = connect(
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
        region_name=config.AWS_DEFAULT_REGION,
        s3_staging_dir=config.ATHENA_RESULTS_BUCKET,
        schema_name=config.ATHENA_DATABASE,
        cursor_class=PandasCursor,
    ).cursor()

    return cursor.execute(sql).as_pandas()
