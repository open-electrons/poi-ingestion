import importlib
import sys


def reload_config(monkeypatch):
    # Patch dotenv BEFORE importing module
    monkeypatch.setattr("dotenv.load_dotenv", lambda *args, **kwargs: None)

    if "poi_ingestion.config" in sys.modules:
        del sys.modules["poi_ingestion.config"]

    return importlib.import_module("poi_ingestion.config")


def test_defaults_when_env_missing(monkeypatch):
    monkeypatch.delenv("OPENCHARGEMAP_API_KEY", raising=False)
    monkeypatch.delenv("DATA_DIR", raising=False)

    config = reload_config(monkeypatch)

    assert config.OPENCHARGEMAP_API_KEY == ""
    assert config.DATA_DIR == "data"


def test_env_overrides(monkeypatch):
    monkeypatch.setenv("OPENCHARGEMAP_API_KEY", "test_key")
    monkeypatch.setenv("DATA_DIR", "/tmp/data")

    config = reload_config(monkeypatch)

    assert config.OPENCHARGEMAP_API_KEY == "test_key"
    assert config.DATA_DIR == "/tmp/data"
