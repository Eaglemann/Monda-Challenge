from contextlib import contextmanager
from types import SimpleNamespace

from utils.snowflake_helpers import SnowflakeHelper


class FakeCursor:
    def __init__(
        self,
        fetch_results: list[tuple[int, ...]] | None = None,
        fetchall_results: list[list[tuple[str]]] | None = None,
    ):
        self.fetch_results = fetch_results or []
        self.fetchall_results = fetchall_results or []
        self.executed_sql: list[str] = []
        self.executed_params: list[tuple | None] = []
        self._idx = 0
        self._fetchall_idx = 0

    def execute(self, sql: str, params=None):
        self.executed_sql.append(sql)
        self.executed_params.append(params)
        return self

    def fetchone(self):
        if self._idx >= len(self.fetch_results):
            return (0,)
        value = self.fetch_results[self._idx]
        self._idx += 1
        return value

    def fetchall(self):
        if self._fetchall_idx >= len(self.fetchall_results):
            return []
        value = self.fetchall_results[self._fetchall_idx]
        self._fetchall_idx += 1
        return value


class FakeConnection:
    def __init__(self, cursor: FakeCursor):
        self._cursor = cursor

    def cursor(self) -> FakeCursor:
        return self._cursor

    def close(self) -> None:
        return None


def build_helper() -> SnowflakeHelper:
    cfg = SimpleNamespace(
        account="acc",
        user="user",
        password="pwd",
        warehouse="wh",
        database="ETL_DB",
        schema_="PUBLIC",
        role=None,
    )
    return SnowflakeHelper(cfg)


def patch_connection(helper: SnowflakeHelper, cursor: FakeCursor) -> None:
    conn = FakeConnection(cursor)

    @contextmanager
    def fake_get_connection():
        yield conn

    helper.get_connection = fake_get_connection  # type: ignore[assignment]


def test_merge_sql_uses_change_predicate_and_qualification() -> None:
    helper = build_helper()
    cursor = FakeCursor(fetch_results=[(1, 0)])
    patch_connection(helper, cursor)

    result = helper.merge_from_staging(
        target_table="events",
        staging_table="events_staging",
        merge_key="id",
        column_types={"id": "NUMBER", "name": "VARCHAR", "event_metadata": "VARIANT"},
    )

    assert result == {"inserted": 1, "updated": 0, "affected": 1}
    assert len(cursor.executed_sql) == 2
    assert '"ETL_DB"."PUBLIC"."events"' in cursor.executed_sql[0]
    assert '"ETL_DB"."PUBLIC"."events_staging"' in cursor.executed_sql[0]
    assert "WHEN MATCHED AND (" in cursor.executed_sql[0]
    assert "IS DISTINCT FROM" in cursor.executed_sql[0]
    assert "RESULT_SCAN" in cursor.executed_sql[1]


def test_merge_with_only_key_column_skips_update_clause() -> None:
    helper = build_helper()
    cursor = FakeCursor(fetch_results=[(2, 0)])
    patch_connection(helper, cursor)

    result = helper.merge_from_staging(
        target_table="events",
        staging_table="events_staging",
        merge_key="id",
        column_types={"id": "NUMBER"},
    )

    assert result == {"inserted": 2, "updated": 0, "affected": 2}
    assert len(cursor.executed_sql) == 2
    assert "WHEN MATCHED" not in cursor.executed_sql[0]
    assert "RESULT_SCAN" in cursor.executed_sql[1]


def test_secure_view_sql_flattens_variant_fields() -> None:
    helper = build_helper()
    cursor = FakeCursor(fetchall_results=[[("id",), ("country",), ("event_metadata",)]])
    patch_connection(helper, cursor)

    helper.create_secure_view(
        view_name="germany_events",
        source_table="events",
        where_clause="country = 'DE'",
        flatten_columns=[{"path": "event_metadata.user_id", "type": "NUMBER"}],
    )

    assert len(cursor.executed_sql) == 2
    ddl = cursor.executed_sql[1]
    assert "CREATE OR REPLACE SECURE VIEW" in ddl
    assert 'src."event_metadata" AS EVENT_METADATA' in ddl
    assert (
        'TRY_PARSE_JSON(TO_VARCHAR(base.EVENT_METADATA)):"user_id"::NUMBER AS EVENT_METADATA_USER_ID'
        in ddl
    )
    assert "WHERE country = 'DE'" in ddl


def test_create_stage_escapes_credentials_and_url() -> None:
    helper = build_helper()
    cursor = FakeCursor()
    patch_connection(helper, cursor)

    helper.create_stage(
        stage_name="ext_stage",
        url="s3://bucket/it's/data",
        credentials={"access_key": "key'123", "secret_key": "secret'oops"},
        endpoint_url="https://minio.local/path?x='1'",
    )

    ddl = cursor.executed_sql[0]
    assert "URL = 's3://bucket/it''s/data'" in ddl
    assert "AWS_KEY_ID = 'key''123'" in ddl
    assert "AWS_SECRET_KEY = 'secret''oops'" in ddl
    assert "ENDPOINT = 'https://minio.local/path?x=''1'''" in ddl


def test_date_cast_expr_uses_explicit_formats() -> None:
    expr = SnowflakeHelper.build_cast_expr('stg."event_date"', "DATE")
    assert expr.startswith("CASE ")
    assert "TRY_TO_DATE(TRIM(stg.\"event_date\"), 'YYYY-MM-DD')" in expr
    assert (
        "REGEXP_LIKE(TRIM(stg.\"event_date\"), '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2}$')"
        in expr
    )
    assert "SPLIT_PART(TRIM(stg.\"event_date\"), '/', 3)" in expr
    assert 'ELSE TRY_TO_DATE(TRIM(stg."event_date"))' in expr
