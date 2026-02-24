import requests
import logging


def extract_open_meteo(lat: float, lon: float, daily: list[str], timezone: str) -> dict | None:
    try:
        url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&daily={','.join(daily)}&timezone={timezone}"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.warning(f"Error fetching data from OpenMeteo: {e}")
        return None

def extract_all_open_meteo(config: dict) -> list[dict]:
    data = []
    for location in config['locations']:
        locations_data = extract_open_meteo(location['latitude'], location['longitude'], config['weather']['daily_variables'], config['weather']['timezone'])
        if locations_data is not None:
            data.append({
                "location": location['name'],
                "data": locations_data
            })
    return data

if __name__ == "__main__":
    from src.utils.config import load_config
    config = load_config()
    logging.info(extract_all_open_meteo(config))