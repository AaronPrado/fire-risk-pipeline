from unittest.mock import patch, Mock
import requests
import pytest
from src.extractors.open_meteo import extract_open_meteo, extract_all_open_meteo


# Tests for extract_open_meteo

class TestExtractOpenMeteo:

    @patch("src.extractors.open_meteo.requests.get")
    def test_returns_json_on_success(self, mock_get):
        mock_response = Mock()
        mock_response.json.return_value = {"daily": {"temperature_2m_max": [25.0]}}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = extract_open_meteo(43.36, -8.41, ["temperature_2m_max"], "Europe/Madrid")

        assert result == {"daily": {"temperature_2m_max": [25.0]}}
        mock_get.assert_called_once()

    @patch("src.extractors.open_meteo.requests.get")
    def test_returns_none_on_http_error(self, mock_get):
        mock_get.side_effect = requests.exceptions.ConnectionError("Connection error")

        result = extract_open_meteo(43.36, -8.41, ["temperature_2m_max"], "Europe/Madrid")

        assert result is None

    @patch("src.extractors.open_meteo.requests.get")
    def test_builds_correct_url(self, mock_get):
        mock_response = Mock()
        mock_response.json.return_value = {}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        extract_open_meteo(43.36, -8.41, ["temperature_2m_max", "precipitation_sum"], "Europe/Madrid")

        called_url = mock_get.call_args[0][0]
        assert "latitude=43.36" in called_url
        assert "longitude=-8.41" in called_url
        assert "temperature_2m_max,precipitation_sum" in called_url
        assert "Europe/Madrid" in called_url


# Tests for extract_all_open_meteo

class TestExtractAllOpenMeteo:

    @pytest.fixture
    def config(self):
        return {
            "locations": [
                {"name": "Ourense", "latitude": 42.34, "longitude": -7.86},
                {"name": "Lugo", "latitude": 43.01, "longitude": -7.56},
            ],
            "weather": {
                "daily_variables": ["temperature_2m_max"],
                "timezone": "Europe/Madrid",
            },
        }

    @patch("src.extractors.open_meteo.extract_open_meteo")
    def test_returns_all_locations(self, mock_extract, config):
        mock_extract.return_value = {"daily": {"temperature_2m_max": [25.0]}}

        result = extract_all_open_meteo(config)

        assert len(result) == 2
        assert result[0]["location"] == "Ourense"
        assert result[1]["location"] == "Lugo"

    @patch("src.extractors.open_meteo.extract_open_meteo")
    def test_skips_failed_locations(self, mock_extract, config):
        mock_extract.side_effect = [
            {"daily": {"temperature_2m_max": [25.0]}},
            None,  # Lugo fails
        ]

        result = extract_all_open_meteo(config)

        assert len(result) == 1
        assert result[0]["location"] == "Ourense"

    @patch("src.extractors.open_meteo.extract_open_meteo")
    def test_wraps_data_with_location_name(self, mock_extract, config):
        api_response = {"daily": {"temperature_2m_max": [30.0]}}
        mock_extract.return_value = api_response

        result = extract_all_open_meteo(config)

        assert result[0] == {"location": "Ourense", "data": api_response}
