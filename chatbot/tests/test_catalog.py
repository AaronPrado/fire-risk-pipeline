from chatbot.src.schema.catalog import TABLE


def test_catalog_has_required_keys():
    for key in ("name", "columns", "partitions", "notes", "risk_level_mapping"):
        assert key in TABLE, f"Key '{key}' not found in catalog"


def test_catalog_table_name():
    assert TABLE["name"] == "fire_risk.daily_risk"


def test_catalog_has_all_partitions():
    partition_names = [p["name"] for p in TABLE["partitions"]]
    for col in ("year", "month", "day"):
        assert col in partition_names, f"Partition '{col}' not found"


def test_catalog_has_main_columns():
    column_names = [c["name"] for c in TABLE["columns"]]
    required = ["time", "location", "risk_index", "risk_level"]
    for col in required:
        assert col in column_names, f"Column '{col}' not found"


def test_catalog_columns_have_name_type_and_description():
    for col in TABLE["columns"]:
        assert "name" in col
        assert "type" in col
        assert "description" in col
        assert col["description"] != "", f"Column '{col['name']}' has empty description"


def test_catalog_partitions_have_name_type_and_description():
    for part in TABLE["partitions"]:
        assert "name" in part
        assert "type" in part
        assert "description" in part


def test_catalog_risk_level_mapping_is_complete():
    mapping = TABLE["risk_level_mapping"]
    expected = {
        "bajo": "low",
        "moderado": "moderate",
        "alto": "high",
        "muy alto": "very_high",
        "extremo": "extreme",
    }
    for es, en in expected.items():
        assert mapping.get(es) == en, f"Mapping '{es}' -> '{en}' is incorrect"
