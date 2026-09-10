from pathlib import Path

from config import Settings


def test_local_input_path_uses_configured_csv(tmp_path: Path):
    dataset = tmp_path / "orders.csv"
    settings = Settings(input_csv=str(dataset))

    assert settings.get_input_path() == dataset.resolve().as_uri()


def test_pipeline_config_defaults():
    from config import PipelineConfig

    cfg = PipelineConfig(execution_mode="incremental", use_scd2=True, batch_id="B123")
    assert cfg.execution_mode == "incremental"
    assert cfg.use_scd2 is True
    assert cfg.batch_id == "B123"
