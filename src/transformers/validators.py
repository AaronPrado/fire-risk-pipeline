import pandas as pd
import logging

logger = logging.getLogger(__name__)

VALIDATION_RULES = {
    "temperature_2m_max": {"min": -20, "max": 50},
    "temperature_2m_min": {"min": -20, "max": 50},
    "relative_humidity_2m_mean": {"min": 0, "max": 100},
    "precipitation_sum": {"min": 0, "max": None},
    "wind_speed_10m_max": {"min": 0, "max": None},
    "wind_gusts_10m_max": {"min": 0, "max": None},
    "et0_fao_evapotranspiration": {"min": 0, "max": None},
}


def _apply_range_filters(df: pd.DataFrame) -> pd.DataFrame:
    initial_rows = len(df)
    mask = pd.Series(True, index=df.index)

    for column, limits in VALIDATION_RULES.items():
        if column not in df.columns:
            continue
        if limits["min"] is not None:
            mask &= df[column] >= limits["min"]
        if limits["max"] is not None:
            mask &= df[column] <= limits["max"]

    filtered_df = df[mask]
    dropped = initial_rows - len(filtered_df)

    if dropped > 0:
        logger.warning(f"Dropped {dropped}/{initial_rows} rows with out-of-range values")

    return filtered_df


def validate_weather_data(data: list[dict]) -> pd.DataFrame:
    dfs = []

    for location in data:
        location_name = location["location"]
        daily = location["data"]["daily"]

        df = pd.DataFrame(daily)
        df["location"] = location_name
        dfs.append(df)

    combined_df = pd.concat(dfs, ignore_index=True)

    nulls_before = combined_df.isnull().sum().sum()
    if nulls_before > 0:
        logger.warning(f"Found {nulls_before} null values, dropping affected rows")
        combined_df = combined_df.dropna()

    validated_df = _apply_range_filters(combined_df)

    logger.info(f"Validation complete: {len(validated_df)} valid rows from {len(data)} locations")

    return validated_df
