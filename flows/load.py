from typing import Dict
from pathlib import Path
import pandas as pd
from prefect import task
from config.settings import SnowflakeSettings
from utils.type_inference import TypeInferenceEngine
from utils.snowflake_helpers import SnowflakeHelper


@task
def infer_schema(df: pd.DataFrame) -> Dict[str, str]:
    """Infer target Snowflake types from the sampled CSV payload."""
    engine = TypeInferenceEngine()
    return engine.infer_types(df)


@task(retries=2)
def create_parent_table(
    table_name: str, column_types: Dict[str, str], sf_config: SnowflakeSettings
) -> None:
    """Ensure target table exists with `id` as the merge-key invariant."""
    helper = SnowflakeHelper(sf_config)
    helper.create_table(table_name, column_types, primary_key="id")


@task(retries=2)
def setup_internal_stage(stage_name: str, sf_config: SnowflakeSettings) -> None:
    """
    Ensure internal stage exists for local-first runs.

    Snowflake cannot read localhost MinIO directly, so local files are pushed
    to an internal stage before COPY.
    """
    helper = SnowflakeHelper(sf_config)
    helper.create_internal_stage(stage_name)


@task(retries=2)
def load_data(
    table_name: str,
    stage_name: str,
    file_path: str,
    column_types: Dict[str, str],
    sf_config: SnowflakeSettings,
) -> dict:
    """
    Load staged CSV data into target table via staging + MERGE.

    This keeps staging table lifecycle, stage PUT/COPY, and MERGE stats in one
    Snowflake session for consistency.
    """
    helper = SnowflakeHelper(sf_config)
    staging_table = f"{table_name}_staging"
    stage_file = Path(file_path).name

    # One session keeps TEMP table scope and LAST_QUERY_ID-based merge stats aligned.
    with helper.get_connection() as conn:
        helper.create_temp_staging_table(staging_table, column_types, conn=conn)
        try:
            helper.put_file_to_stage(stage_name, file_path, conn=conn)
            helper.copy_into_staging(staging_table, stage_name, stage_file, conn=conn)
            return helper.merge_from_staging(
                table_name,
                staging_table,
                merge_key="id",
                column_types=column_types,
                conn=conn,
            )
        finally:
            try:
                # Cleanup is best effort as stale stage files should not fail a successful load.
                helper.remove_from_stage(stage_name, stage_file, conn=conn)
            except Exception:
                pass
