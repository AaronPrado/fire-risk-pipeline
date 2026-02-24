import yaml

def load_config(config_path: str = "/opt/airflow/configs/config.yaml") -> dict:

    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Config file not found at {config_path}")
    except yaml.YAMLError as exc:
        raise ValueError(f"Error loading config file: {exc}")

    return config