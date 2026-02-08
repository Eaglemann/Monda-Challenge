from types import SimpleNamespace

import pandas as pd

import flows.pipeline as pipeline


def test_pipeline_smoke_with_mocked_dependencies(monkeypatch, tmp_path) -> None:
    csv_path = tmp_path / "events.csv"
    csv_path.write_text(
        "id,name,country,event_type,event_date,event_metadata\n"
        '1,Ana,ES,signup,2025-01-03,"{""user_id"": 789, ""session_duration"": 30}"\n',
        encoding="utf-8",
    )

    calls: dict[str, object] = {}

    monkeypatch.setattr(pipeline, "MinIOSettings", lambda: SimpleNamespace())
    monkeypatch.setattr(
        pipeline,
        "SnowflakeSettings",
        lambda: SimpleNamespace(database="ETL_DB", schema_="PUBLIC"),
    )

    def fake_upload_to_minio(file_path, object_name, _minio_cfg):
        calls["upload"] = (file_path, object_name)
        return f"s3://raw/{object_name}"

    def fake_download_from_minio(object_name, output_path, _minio_cfg):
        calls["download"] = (object_name, output_path)
        return pd.read_csv(csv_path)

    def fake_infer_schema(_df):
        return {
            "id": "NUMBER",
            "name": "VARCHAR",
            "country": "VARCHAR",
            "event_type": "VARCHAR",
            "event_date": "DATE",
            "event_metadata": "VARIANT",
        }

    def fake_create_parent_table(table_name, _column_types, _sf_cfg):
        calls["table"] = table_name

    def fake_setup_internal_stage(stage_name, _sf_cfg):
        calls["stage"] = stage_name

    def fake_load_data(table_name, stage_name, local_path, _column_types, _sf_cfg):
        calls["load"] = (table_name, stage_name, local_path)
        return {"affected": 1, "inserted": 1, "updated": 0}

    monkeypatch.setattr(pipeline, "upload_to_minio", fake_upload_to_minio)
    monkeypatch.setattr(pipeline, "download_from_minio", fake_download_from_minio)
    monkeypatch.setattr(pipeline, "infer_schema", fake_infer_schema)
    monkeypatch.setattr(pipeline, "create_parent_table", fake_create_parent_table)
    monkeypatch.setattr(pipeline, "setup_internal_stage", fake_setup_internal_stage)
    monkeypatch.setattr(pipeline, "load_data", fake_load_data)
    monkeypatch.setattr(pipeline, "validate_row_count", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(pipeline, "sample_rows", lambda *_args, **_kwargs: None)

    result = pipeline.etl_pipeline.fn(str(csv_path), table_name="events")

    assert result["table"] == "events"
    assert result["rows"] == 1
    assert result["inserted"] == 1
    assert result["updated"] == 0
    assert result["validation_passed"] is True
    assert result["input_file"] == "events.csv"
    assert result["column_count"] == 6

    assert calls["upload"] == (str(csv_path), "uploads/events.csv")
    assert calls["stage"] == "local_stage"
