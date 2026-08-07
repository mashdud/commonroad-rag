
from commonroad_pipeline.config import load_config


def test_default_config_paths():
    cfg = load_config()
    assert cfg.base_path.startswith("abfss://")
    assert cfg.raw_path.endswith("/raw")
    assert cfg.bronze_table.endswith(".bronze.scenario_raw")


def test_config_respects_env_override(monkeypatch):
    monkeypatch.setenv("CATALOG", "commonroad_test")
    cfg = load_config()
    assert cfg.catalog == "commonroad_test"
    assert cfg.bronze_table == "commonroad_test.bronze.scenario_raw"
