import pandas as pd
import pytest
from src.transformers.risk_calculator import (
    _apply_normalization,
    _apply_weights,
    _get_risk_level,
    _apply_seasonal_factor,
    calculate_fire_risk,
)


# Fixtures 

@pytest.fixture
def config():
    """Minimal config.yaml structure mirror."""
    return {
        "risk": {
            "normalization": {
                "temperature_2m_max": {"min": 0, "max": 50},
                "relative_humidity_2m_mean": {"min": 0, "max": 100},
                "precipitation_sum": {"min": 0, "max": 50},
                "wind_speed_10m_max": {"min": 0, "max": 100},
            },
            "weights": {
                "temperature_2m_max": 0.30,
                "relative_humidity_2m_mean": 0.30,
                "precipitation_sum": 0.20,
                "wind_speed_10m_max": 0.20,
            },
            "seasonal_factor": {
                1: 0.60, 2: 0.75, 3: 0.85, 4: 0.75, 5: 0.70, 6: 0.85,
                7: 1.00, 8: 1.15, 9: 1.10, 10: 1.00, 11: 0.70, 12: 0.60,
            },
            "thresholds": {
                "low": 0.2,
                "moderate": 0.4,
                "high": 0.6,
                "very_high": 0.8,
                "extreme": 1.0,
            },
        }
    }


@pytest.fixture
def sample_df():
    """DF with weather data for testing."""
    return pd.DataFrame({
        "time": ["2025-08-10", "2025-01-15"],
        "location": ["Ourense", "A Coruña"],
        "temperature_2m_max": [40.0, 10.0],
        "relative_humidity_2m_mean": [20.0, 80.0],
        "precipitation_sum": [0.0, 30.0],
        "wind_speed_10m_max": [50.0, 10.0],
    })


# Tests for _apply_normalization 

class TestApplyNormalization:

    def test_normalizes_values_to_0_1_range(self, sample_df, config):
        result = _apply_normalization(sample_df.copy(), config)

        # temperature: 40/50 = 0.8
        assert result["temperature_2m_max"].iloc[0] == pytest.approx(0.8)
        # temperature: 10/50 = 0.2
        assert result["temperature_2m_max"].iloc[1] == pytest.approx(0.2)

    def test_inverts_humidity_and_precipitation(self, sample_df, config):
        result = _apply_normalization(sample_df.copy(), config)

        # humidity 20/100 = 0.2 → inverted = 0.8
        assert result["relative_humidity_2m_mean"].iloc[0] == pytest.approx(0.8)
        # precipitation 0/50 = 0.0 → inverted = 1.0
        assert result["precipitation_sum"].iloc[0] == pytest.approx(1.0)

    def test_skips_columns_not_in_dataframe(self, config):
        df = pd.DataFrame({"temperature_2m_max": [25.0]})
        result = _apply_normalization(df.copy(), config)
        assert "wind_speed_10m_max" not in result.columns


# Tests for _apply_weights 

class TestApplyWeights:

    def test_multiplies_by_weights(self, config):
        df = pd.DataFrame({
            "temperature_2m_max": [1.0],
            "relative_humidity_2m_mean": [1.0],
            "precipitation_sum": [1.0],
            "wind_speed_10m_max": [1.0],
        })
        result = _apply_weights(df.copy(), config)

        assert result["temperature_2m_max"].iloc[0] == pytest.approx(0.30)
        assert result["relative_humidity_2m_mean"].iloc[0] == pytest.approx(0.30)
        assert result["precipitation_sum"].iloc[0] == pytest.approx(0.20)
        assert result["wind_speed_10m_max"].iloc[0] == pytest.approx(0.20)

    def test_weights_sum_to_one(self, config):
        weights = config["risk"]["weights"]
        assert sum(weights.values()) == pytest.approx(1.0)


# Tests for _get_risk_level

class TestGetRiskLevel:

    @pytest.mark.parametrize("risk_index, expected_level", [
        (0.0, "low"),
        (0.19, "low"),
        (0.4, "moderate"),
        (0.59, "moderate"),
        (0.6, "high"),
        (0.79, "high"),
        (0.8, "very_high"),
        (0.99, "very_high"),
        (1.0, "extreme"),
    ])
    def test_risk_level_thresholds(self, risk_index, expected_level, config):
        assert _get_risk_level(risk_index, config) == expected_level


# Tests for _apply_seasonal_factor

class TestApplySeasonalFactor:

    def test_august_amplifies_risk(self, config):
        df = pd.DataFrame({
            "time": ["2025-08-15"],
            "risk_index": [0.50],
        })
        result = _apply_seasonal_factor(df.copy(), config)
        # August factor = 1.15 → 0.50 * 1.15 = 0.575
        assert result["risk_index"].iloc[0] == pytest.approx(0.575)

    def test_january_reduces_risk(self, config):
        df = pd.DataFrame({
            "time": ["2025-01-15"],
            "risk_index": [0.50],
        })
        result = _apply_seasonal_factor(df.copy(), config)
        # January factor = 0.60 → 0.50 * 0.60 = 0.30
        assert result["risk_index"].iloc[0] == pytest.approx(0.30)

    def test_clips_to_0_1(self, config):
        df = pd.DataFrame({
            "time": ["2025-08-15"],
            "risk_index": [0.95],
        })
        result = _apply_seasonal_factor(df.copy(), config)
        # 0.95 * 1.15 = 1.0925 → clipped to 1.0
        assert result["risk_index"].iloc[0] == 1.0


# Tests for calculate_fire_risk

class TestCalculateFireRisk:

    def test_returns_risk_index_and_level_columns(self, sample_df, config):
        result = calculate_fire_risk(sample_df.copy(), config)
        assert "risk_index" in result.columns
        assert "risk_level" in result.columns

    def test_risk_index_between_0_and_1(self, sample_df, config):
        result = calculate_fire_risk(sample_df.copy(), config)
        assert (result["risk_index"] >= 0).all()
        assert (result["risk_index"] <= 1).all()

    def test_hot_dry_windy_higher_risk_than_cold_wet_calm(self, sample_df, config):
        result = calculate_fire_risk(sample_df.copy(), config)
        # Row 0: Ourense in August (hot, dry, windy) should be higher risk
        # Row 1: A Coruña in January (cold, wet, calm) should be lower risk
        assert result["risk_index"].iloc[0] > result["risk_index"].iloc[1]
