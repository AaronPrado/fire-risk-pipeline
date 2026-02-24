import pandas as pd

def _apply_normalization(df: pd.DataFrame, config: dict) -> pd.DataFrame:

    INVERSE_COLUMNS = ["relative_humidity_2m_mean", "precipitation_sum"]

    for col in config["risk"]["weights"]:
        if col not in df.columns:
            continue
        
        col_min = config["risk"]["normalization"][col]["min"]
        col_max = config["risk"]["normalization"][col]["max"]
        df[col] = (df[col] - col_min) / (col_max - col_min)

        if col in INVERSE_COLUMNS:
            df[col] = 1.0 - df[col]
    
    return df

def _apply_weights(df: pd.DataFrame, config: dict) -> pd.DataFrame:

    for col in config["risk"]["weights"]:
        if col not in df.columns:
            continue
        
        df[col] = df[col] * config["risk"]["weights"][col]
    
    return df

def _get_risk_level(risk_index: float, config: dict) -> str:
    if risk_index < config["risk"]["thresholds"]["moderate"]:
        return "low"
    elif risk_index < config["risk"]["thresholds"]["high"]:
        return "moderate"
    elif risk_index < config["risk"]["thresholds"]["very_high"]:
        return "high"
    elif risk_index < config["risk"]["thresholds"]["extreme"]:
        return "very_high"
    else:
        return "extreme"

def _apply_seasonal_factor(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    months = pd.to_datetime(df["time"]).dt.month
    seasonal_factors = config["risk"]["seasonal_factor"]
    df["risk_index"] = df["risk_index"] * months.map(seasonal_factors)
    df["risk_index"] = df["risk_index"].clip(0, 1)
    return df


def calculate_fire_risk(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Calcula el índice de riesgo de incendio forestal (0-1)
    """
    df = _apply_normalization(df, config)
    df = _apply_weights(df, config)

    weight_columns = list(config["risk"]["weights"].keys())
    df["risk_index"] = df[weight_columns].sum(axis=1)

    df = _apply_seasonal_factor(df, config)

    df["risk_level"] = df["risk_index"].apply(lambda x: _get_risk_level(x, config))

    return df

