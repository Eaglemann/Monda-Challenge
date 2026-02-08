from typing import List
from prefect import task

from config.settings import SnowflakeSettings
from utils.snowflake_helpers import SnowflakeHelper


# Runtime guardrails for ETL sanity checks, not a full data-quality framework.
@task
def validate_row_count(
    table_name: str, expected_min: int, sf_config: SnowflakeSettings
) -> bool:
    """Guard against empty/near-empty loads that indicate ingestion failure."""

    helper = SnowflakeHelper(sf_config)
    table_name_q = helper.qualify_name(table_name)

    with helper.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name_q}")
        count = cursor.fetchone()[0]

    if count < expected_min:
        raise ValueError(f"Row count {count} below minimum {expected_min}")

    print(f"Row count OK ({count})")
    return True


@task
def validate_no_nulls(
    table_name: str, columns: List[str], sf_config: SnowflakeSettings
) -> bool:
    """Check required business columns for null regressions after load."""

    helper = SnowflakeHelper(sf_config)
    table_name_q = helper.qualify_name(table_name)

    for col in columns:
        col_q = helper.quote_identifier(col)
        with helper.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name_q} WHERE {col_q} IS NULL")
            null_count = cursor.fetchone()[0]

        if null_count > 0:
            raise ValueError(f"Found {null_count} nulls in {col}")

    print("Null check OK")
    return True


@task
def sample_rows(table_name: str, limit: int, sf_config: SnowflakeSettings) -> None:
    """Print a bounded sample to verify loaded shape without full-table scans."""

    helper = SnowflakeHelper(sf_config)
    table_name_q = helper.qualify_name(table_name)

    with helper.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name_q} LIMIT {limit}")
        rows = cursor.fetchall()

    if not rows:
        print(f"Sample {table_name}: no rows")
        return

    print(f"Sample {table_name} (limit {limit})")
    for row in rows:
        print(f"  {row}")


def _print_block(title: str, lines: List[str]) -> None:
    joined = ", ".join(lines)
    print(f"{title}: {joined}")


def summarize_run(
    input_file: str,
    object_name: str,
    table: str,
    rows: int,
    views: List[str] | None,
    validation_passed: bool,
    inserted: int | None = None,
    updated: int | None = None,
    column_count: int | None = None,
) -> str:
    """Emit a compact run summary used by local operators and challenge reviewers."""
    views = views or []

    _print_block(
        "Input",
        [
            f"CSV={input_file}",
            f"MinIO={object_name}",
        ],
    )

    schema_lines = []
    if column_count is not None:
        schema_lines.append(f"Columns inferred: {column_count}")
    if schema_lines:
        _print_block("Schema", schema_lines)

    _print_block(
        "Load",
        [
            f"Table={table}",
            f"Affected={rows}",
            f"Inserts={inserted}" if inserted is not None else "Inserts=n/a",
            f"Updates={updated}" if updated is not None else "Updates=n/a",
        ],
    )

    _print_block(
        "Validation",
        [f"{'passed' if validation_passed else 'not run'}"],
    )

    _print_block(
        "Subsets",
        [f"Views={', '.join(views)}" if views else "Views=none"],
    )

    _print_block(
        "Summary",
        [
            f"Table={table}",
            f"Affected={rows}",
            f"Validation={'passed' if validation_passed else 'not run'}",
        ],
    )

    return "ok"
