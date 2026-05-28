import yaml


def load_config(config_path: str = "/opt/airflow/configs/config.yaml") -> dict:

    try:
        with open(config_path) as f:
            config = yaml.safe_load(f)
    except FileNotFoundError as err:
        raise FileNotFoundError(f"Config file not found at {config_path}") from err
    except yaml.YAMLError as exc:
        raise ValueError(f"Error loading config file: {exc}") from exc

    return config
