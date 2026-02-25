import pandas as pd
import pytest
from src.transformers.validators import _apply_range_filters, validate_weather_data


# Tests for _apply_range_filters

class TestApplyRangeFilters:

    def test_keeps_valid_rows(self):
        df = pd.DataFrame({
            "temperature_2m_max": [25.0, 30.0],
            "relative_humidity_2m_mean": [50.0, 70.0],
            "precipitation_sum": [5.0, 10.0],
        })
        result = _apply_range_filters(df)
        assert len(result) == 2

    def test_drops_rows_with_temperature_out_of_range(self):
        df = pd.DataFrame({
            "temperature_2m_max": [25.0, 55.0, -25.0],
            "relative_humidity_2m_mean": [50.0, 50.0, 50.0],
        })
        result = _apply_range_filters(df)
        assert len(result) == 1
        assert result["temperature_2m_max"].iloc[0] == 25.0

    def test_drops_rows_with_negative_precipitation(self):
        df = pd.DataFrame({
            "precipitation_sum": [5.0, -1.0, 0.0],
        })
        result = _apply_range_filters(df)
        assert len(result) == 2

    def test_allows_unlimited_upper_bound(self):
        # precipitation_sum has max: None → no upper limit
        df = pd.DataFrame({
            "precipitation_sum": [0.0, 500.0, 9999.0],
        })
        result = _apply_range_filters(df)
        assert len(result) == 3

    def test_ignores_columns_not_in_rules(self):
        df = pd.DataFrame({
            "unknown_column": [-999, 999],
            "temperature_2m_max": [20.0, 25.0],
        })
        result = _apply_range_filters(df)
        assert len(result) == 2

    def test_boundary_values_are_included(self):
        df = pd.DataFrame({
            "temperature_2m_max": [-20.0, 50.0],
            "relative_humidity_2m_mean": [0.0, 100.0],
        })
        result = _apply_range_filters(df)
        assert len(result) == 2


# Tests for validate_weather_data

class TestValidateWeatherData:

    @pytest.fixture
    def raw_api_data(self):
        """Simulates the list[dict] structure coming from extract_all_open_meteo."""
        return [
            {
                "location": "Ourense",
                "data": {
                    "daily": {
                        "time": ["2025-08-10", "2025-08-11"],
                        "temperature_2m_max": [35.0, 38.0],
                        "relative_humidity_2m_mean": [40.0, 35.0],
                        "precipitation_sum": [0.0, 0.0],
                    }
                },
            },
            {
                "location": "Lugo",
                "data": {
                    "daily": {
                        "time": ["2025-08-10", "2025-08-11"],
                        "temperature_2m_max": [28.0, 30.0],
                        "relative_humidity_2m_mean": [60.0, 55.0],
                        "precipitation_sum": [2.0, 0.0],
                    }
                },
            },
        ]

    def test_combines_all_locations(self, raw_api_data):
        result = validate_weather_data(raw_api_data)
        assert len(result) == 4
        assert set(result["location"].unique()) == {"Ourense", "Lugo"}

    def test_adds_location_column(self, raw_api_data):
        result = validate_weather_data(raw_api_data)
        assert "location" in result.columns

    def test_drops_null_rows(self):
        data = [
            {
                "location": "Vigo",
                "data": {
                    "daily": {
                        "time": ["2025-08-10", "2025-08-11"],
                        "temperature_2m_max": [30.0, None],
                        "relative_humidity_2m_mean": [50.0, 50.0],
                    }
                },
            }
        ]
        result = validate_weather_data(data)
        assert len(result) == 1

    def test_drops_invalid_rows_after_null_removal(self):
        data = [
            {
                "location": "Ferrol",
                "data": {
                    "daily": {
                        "time": ["2025-08-10", "2025-08-11"],
                        "temperature_2m_max": [25.0, 999.0],  # 999 out of range
                        "relative_humidity_2m_mean": [50.0, 50.0],
                    }
                },
            }
        ]
        result = validate_weather_data(data)
        assert len(result) == 1
