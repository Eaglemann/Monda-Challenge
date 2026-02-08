import os
import tempfile
from pathlib import Path

from prefect import flow

from config.settings import MinIOSettings, SnowflakeSettings
from flows.load import (
    infer_schema,
    create_parent_table,
    setup_internal_stage,
    load_data,
)
from utils.minio_helpers import download_from_minio, upload_to_minio
from utils.validators import validate_row_count, sample_rows


def resolve_load_inputs(csv_file: str | None) -> tuple[str, str]:
    """Derive deterministic object naming for idempotent local upload/download runs."""
    if not csv_file:
        raise ValueError("csv_file is required.")
    object_name = f"uploads/{Path(csv_file).name}"
    return object_name, csv_file


def validate_merge_key(column_types: dict[str, str], merge_key: str = "id") -> None:
    """Fail fast when merge key is absent, instead of producing partial load behavior."""
    if merge_key not in column_types:
        raise ValueError(f"Missing required merge key column: {merge_key}")


@flow(name="etl-csv-to-snowflake")
def etl_pipeline(
    csv_file: str | None,
    table_name: str = "events",
):
    """
    Orchestrate the local-first CSV -> Snowflake load workflow.

    Contract:
    - Uses MinIO for local object-store coverage, then loads Snowflake through
      an internal stage.
    - Requires `id` in inferred schema for deterministic merge semantics.
    - Returns run metadata consumed by CLI summary and operational validation.
    """
    minio_cfg = MinIOSettings()
    sf_cfg = SnowflakeSettings()

    object_name, local_csv = resolve_load_inputs(csv_file)
    upload_to_minio(local_csv, object_name, minio_cfg)

    # Persist to a named temp file so Snowflake PUT can read a stable local path.
    tmp_file = tempfile.NamedTemporaryFile(
        suffix=f"_{Path(object_name).name}",
        prefix="etl_",
        delete=False,
    )
    tmp_file.close()
    local_path = tmp_file.name
    try:
        df = download_from_minio(object_name, local_path, minio_cfg)
        column_types = infer_schema(df)

        validate_merge_key(column_types)
        create_parent_table(table_name, column_types, sf_cfg)

        stage_name = "local_stage"
        setup_internal_stage(stage_name, sf_cfg)

        merge_result = load_data(
            table_name,
            stage_name,
            local_path,
            column_types,
            sf_cfg,
        )

        validate_row_count(table_name, expected_min=1, sf_config=sf_cfg)
        sample_rows(table_name, limit=5, sf_config=sf_cfg)

        return {
            "table": table_name,
            "rows": merge_result["affected"],
            "inserted": merge_result["inserted"],
            "updated": merge_result["updated"],
            "validation_passed": True,
            "object_name": object_name,
            "input_file": str(Path(local_csv).name),
            "column_count": len(column_types),
        }
    finally:
        try:
            # Always clean local temp artifacts, even when downstream tasks fail.
            os.remove(local_path)
        except FileNotFoundError:
            pass
