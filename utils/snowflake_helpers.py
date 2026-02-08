from contextlib import contextmanager
import re
from typing import Any, Dict, Iterable, List

import snowflake.connector

from config.settings import SnowflakeSettings


class SnowflakeHelper:
    """Snowflake DDL/DML primitives used by the ETL flows."""

    _CAST_TYPE_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]*(\([0-9,\s]+\))?$")

    def __init__(self, config: SnowflakeSettings):
        self.config = config

    @contextmanager
    def get_connection(self):
        """Open a short-lived Snowflake connection for a single operation scope."""
        conn = snowflake.connector.connect(
            account=self.config.account,
            user=self.config.user,
            password=self.config.password,
            warehouse=self.config.warehouse,
            database=self.config.database,
            schema=self.config.schema_,
            role=self.config.role,
        )
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def _connection(self, conn=None):
        """Reuse a caller-owned connection or fallback to a managed short-lived one."""
        if conn is not None:
            yield conn
            return
        with self.get_connection() as managed_conn:
            yield managed_conn

    @staticmethod
    def quote_identifier(identifier: str) -> str:
        """Quote identifiers to preserve case and avoid keyword/name collisions."""
        if not isinstance(identifier, str):
            raise TypeError("Identifier must be a string.")
        value = identifier.strip()
        if not value:
            raise ValueError("Identifier cannot be empty.")
        return f'"{value.replace('"', '""')}"'

    @staticmethod
    def _escape_string_literal(value: str) -> str:
        """Escape a SQL string literal value safely."""
        if not isinstance(value, str):
            raise TypeError("SQL literal value must be a string.")
        return "'" + value.replace("'", "''") + "'"

    def qualify_name(self, name: str) -> str:
        """Resolve object names into stable `DB.SCHEMA.OBJECT` form."""
        if not isinstance(name, str):
            raise TypeError("Object name must be a string.")

        parts = [part.strip() for part in name.split(".")]
        if any(not part for part in parts):
            raise ValueError(f"Invalid object name: {name}")

        if len(parts) == 1:
            parts = [self.config.database, self.config.schema_, parts[0]]
        elif len(parts) == 2:
            parts = [self.config.database, parts[0], parts[1]]
        elif len(parts) != 3:
            raise ValueError(f"Object name must have up to 3 parts: {name}")

        return ".".join(self.quote_identifier(part) for part in parts)

    def _stage_reference(self, stage_name: str, file_name: str | None = None) -> str:
        stage_ref = f"@{self.qualify_name(stage_name)}"
        if file_name:
            return f"{stage_ref}/{file_name}"
        return stage_ref

    def _name_parts(self, name: str) -> tuple[str, str, str]:
        if not isinstance(name, str):
            raise TypeError("Object name must be a string.")

        parts = [part.strip() for part in name.split(".")]
        if any(not part for part in parts):
            raise ValueError(f"Invalid object name: {name}")

        if len(parts) == 1:
            return self.config.database, self.config.schema_, parts[0]
        if len(parts) == 2:
            return self.config.database, parts[0], parts[1]
        if len(parts) == 3:
            return parts[0], parts[1], parts[2]
        raise ValueError(f"Object name must have up to 3 parts: {name}")

    @classmethod
    def _normalize_cast_type(cls, dtype: str) -> str:
        """Validate cast tokens so generated SQL cannot inject arbitrary clauses."""
        if not isinstance(dtype, str):
            raise TypeError("Data type must be a string.")
        normalized = dtype.strip().upper()
        if not cls._CAST_TYPE_PATTERN.match(normalized):
            raise ValueError(f"Invalid type token: {dtype}")
        return normalized

    def create_table(
        self,
        table_name: str,
        column_types: Dict[str, str],
        primary_key: str | None = None,
        conn=None,
    ) -> None:
        """Create target table shape from inferred schema, preserving merge key contract."""
        table_name_q = self.qualify_name(table_name)
        columns = [
            f"{self.quote_identifier(col)} {self._normalize_cast_type(dtype)}"
            for col, dtype in column_types.items()
        ]

        if primary_key:
            columns.append(f"PRIMARY KEY ({self.quote_identifier(primary_key)})")

        ddl = f"""
        CREATE TABLE IF NOT EXISTS {table_name_q} (
            {", ".join(columns)}
        )
        """
        with self._connection(conn) as active_conn:
            active_conn.cursor().execute(ddl)

    def create_stage(
        self,
        stage_name: str,
        url: str,
        credentials: Dict[str, str],
        endpoint_url: str | None = None,
        conn=None,
    ) -> None:
        """Create an external stage with escaped literals for endpoint and credentials."""
        stage_name_q = self.qualify_name(stage_name)
        endpoint_clause = ""
        if endpoint_url:
            endpoint_clause = f"ENDPOINT = {self._escape_string_literal(endpoint_url)}"

        url_literal = self._escape_string_literal(url)
        key_literal = self._escape_string_literal(credentials["access_key"])
        secret_literal = self._escape_string_literal(credentials["secret_key"])

        ddl = f"""
        CREATE STAGE IF NOT EXISTS {stage_name_q}
        URL = {url_literal}
        CREDENTIALS = (
            AWS_KEY_ID = {key_literal}
            AWS_SECRET_KEY = {secret_literal}
        )
        {endpoint_clause}
        FILE_FORMAT = (
            TYPE = 'CSV'
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        )
        """

        with self._connection(conn) as active_conn:
            active_conn.cursor().execute(ddl)

    def create_internal_stage(self, stage_name: str, conn=None) -> None:
        """Create an internal stage used by the local MinIO -> Snowflake transfer path."""
        stage_name_q = self.qualify_name(stage_name)
        ddl = f"CREATE OR REPLACE STAGE {stage_name_q}"
        with self._connection(conn) as active_conn:
            active_conn.cursor().execute(ddl)

    def put_file_to_stage(self, stage_name: str, file_path: str, conn=None) -> None:
        """Upload local file to stage before COPY into a staging table."""
        stage_ref = self._stage_reference(stage_name)
        sql = f"PUT file://{file_path} {stage_ref} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        with self._connection(conn) as active_conn:
            active_conn.cursor().execute(sql)

    def remove_from_stage(self, stage_name: str, file_name: str, conn=None) -> None:
        """Remove staged files so reruns do not accumulate stale artifacts."""
        stage_ref = self._stage_reference(stage_name, file_name=file_name)
        sql = f"REMOVE {stage_ref}"
        with self._connection(conn) as active_conn:
            active_conn.cursor().execute(sql)

    def create_temp_staging_table(
        self,
        staging_table: str,
        column_types: Dict[str, str],
        conn=None,
    ) -> None:
        """Create session-scoped staging table; all source values land as VARCHAR first."""
        staging_name = self.qualify_name(staging_table)
        columns = [f"{self.quote_identifier(col)} VARCHAR" for col in column_types]
        ddl = f"""
        CREATE OR REPLACE TEMPORARY TABLE {staging_name} (
            {", ".join(columns)}
        )
        """
        with self._connection(conn) as active_conn:
            active_conn.cursor().execute(ddl)

    def copy_into_staging(
        self,
        staging_table: str,
        stage_name: str,
        stage_file: str,
        conn=None,
    ) -> None:
        """COPY staged CSV payload into the staging table."""
        staging_name = self.qualify_name(staging_table)
        stage_ref = self._stage_reference(stage_name, file_name=stage_file)
        sql = f"""
        COPY INTO {staging_name}
        FROM {stage_ref}
        FILE_FORMAT = (
            TYPE = 'CSV'
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        )
        """
        with self._connection(conn) as active_conn:
            active_conn.cursor().execute(sql)

    @staticmethod
    def build_cast_expr(column_expr: str, snowflake_type: str) -> str:
        """Map staging VARCHAR values into target Snowflake types with tolerant parsing."""
        type_upper = snowflake_type.upper()
        if type_upper == "NUMBER":
            return f"TRY_TO_NUMBER({column_expr})"
        if type_upper == "FLOAT":
            return f"TRY_TO_DOUBLE({column_expr})"
        if type_upper == "DATE":
            trimmed = f"TRIM({column_expr})"
            # Snowflake can map 2-digit years into year 0025 depending on session parsing.
            # Normalize MM/DD/YY to MM/DD/20YY to keep event data deterministic.
            mmddyy_as_20yy = (
                f"LPAD(SPLIT_PART({trimmed}, '/', 1), 2, '0') || '/' || "
                f"LPAD(SPLIT_PART({trimmed}, '/', 2), 2, '0') || '/20' || SPLIT_PART({trimmed}, '/', 3)"
            )
            return (
                "CASE "
                f"WHEN REGEXP_LIKE({trimmed}, '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$') "
                f"THEN TRY_TO_DATE({trimmed}, 'YYYY-MM-DD') "
                f"WHEN REGEXP_LIKE({trimmed}, '^[0-9]{{1,2}}/[0-9]{{1,2}}/[0-9]{{2}}$') "
                f"THEN TRY_TO_DATE({mmddyy_as_20yy}, 'MM/DD/YYYY') "
                f"ELSE TRY_TO_DATE({trimmed}) "
                "END"
            )
        if type_upper == "TIMESTAMP_NTZ":
            return f"TRY_TO_TIMESTAMP_NTZ({column_expr})"
        if type_upper == "BOOLEAN":
            return f"TRY_TO_BOOLEAN({column_expr})"
        if type_upper == "VARIANT":
            return f"TRY_PARSE_JSON({column_expr})"
        return f"TO_VARCHAR({column_expr})"

    def merge_from_staging(
        self,
        target_table: str,
        staging_table: str,
        merge_key: str,
        column_types: Dict[str, str],
        conn=None,
    ) -> Dict[str, int]:
        """
        Merge staging rows into target table and return atomic insert/update counts.

        Counts come from `RESULT_SCAN(LAST_QUERY_ID())` in the same session as
        the MERGE, so reported stats reflect the actual mutation statement.
        """
        target_name = self.qualify_name(target_table)
        staging_name = self.qualify_name(staging_table)
        merge_key_q = self.quote_identifier(merge_key)
        columns = list(column_types.keys())

        select_cols = [
            f"{self.build_cast_expr(f'stg.{self.quote_identifier(col)}', dtype)} AS {self.quote_identifier(col)}"
            for col, dtype in column_types.items()
        ]
        src_cte = f"""
        SELECT {", ".join(select_cols)}
        FROM {staging_name} stg
        """

        updatable_columns = [col for col in columns if col != merge_key]
        update_assignments = [
            f"tgt.{self.quote_identifier(col)} = src.{self.quote_identifier(col)}"
            for col in updatable_columns
        ]
        diff_predicate = " OR ".join(
            [
                f"tgt.{self.quote_identifier(col)} IS DISTINCT FROM src.{self.quote_identifier(col)}"
                for col in updatable_columns
            ]
        )

        if updatable_columns:
            when_matched_clause = f"""
            WHEN MATCHED AND ({diff_predicate}) THEN
                UPDATE SET {", ".join(update_assignments)}
            """
        else:
            when_matched_clause = ""

        insert_columns = ", ".join(self.quote_identifier(col) for col in columns)
        insert_values = ", ".join(
            f"src.{self.quote_identifier(col)}" for col in columns
        )
        merge_sql = f"""
        MERGE INTO {target_name} tgt
        USING (
            {src_cte}
        ) src
        ON tgt.{merge_key_q} = src.{merge_key_q}
        {when_matched_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns})
            VALUES ({insert_values})
        """

        with self._connection(conn) as active_conn:
            cursor = active_conn.cursor()
            cursor.execute(merge_sql)
            # Read MERGE action counts from the executed statement itself.
            cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))")
            result = cursor.fetchone()
            inserted = (
                int(result[0])
                if result and len(result) > 0 and result[0] is not None
                else 0
            )
            updated = (
                int(result[1])
                if result and len(result) > 1 and result[1] is not None
                else 0
            )
            return {
                "inserted": inserted,
                "updated": updated,
                "affected": inserted + updated,
            }

    def create_secure_view(
        self,
        view_name: str,
        source_table: str,
        where_clause: str,
        flatten_columns: List[Any] | None = None,
        conn=None,
    ) -> None:
        """
        Create secure subset view with optional flattened JSON projections.

        Source columns are projected to deterministic unquoted aliases first,
        so config filters like `country = 'DE'` remain stable even when the
        underlying table uses quoted/case-sensitive column names.
        """
        source_columns = self.get_table_columns(source_table, conn=conn)
        projection_aliases = self._build_projection_aliases(source_columns)
        alias_lookup = {col.lower(): alias for col, alias in projection_aliases.items()}
        # Normalize source names once in a subquery; outer WHERE stays human-readable.
        select_source_cols = [
            f"src.{self.quote_identifier(col)} AS {alias}"
            for col, alias in projection_aliases.items()
        ]

        select_cols = ["base.*"]
        if flatten_columns:
            for item in self.normalize_flatten_columns(flatten_columns):
                path = item["path"]
                parts = [part.strip() for part in path.split(".") if part.strip()]
                if not parts:
                    raise ValueError("Flatten path cannot be empty.")
                base_part = parts[0]
                aliased_base = alias_lookup.get(base_part.lower(), base_part)
                aliased_path = ".".join([aliased_base, *parts[1:]])
                dtype = self._normalize_cast_type(item["type"])
                col_name = path.replace(".", "_")
                out_alias = self._normalize_unquoted_identifier(col_name)
                json_expr = self._json_extract_expr(aliased_path, source_alias="base")
                select_cols.append(f"{json_expr}::{dtype} AS {out_alias}")

        view_name_q = self.qualify_name(view_name)
        source_name_q = self.qualify_name(source_table)
        ddl = f"""
        CREATE OR REPLACE SECURE VIEW {view_name_q} AS
        SELECT {", ".join(select_cols)}
        FROM (
            SELECT {", ".join(select_source_cols)}
            FROM {source_name_q} src
        ) base
        WHERE {where_clause}
        """
        with self._connection(conn) as active_conn:
            active_conn.cursor().execute(ddl)

    @staticmethod
    def normalize_flatten_columns(
        flatten_columns: Iterable[Any],
    ) -> List[Dict[str, str]]:
        """Normalize flatten config into `{path, type}` entries."""
        normalized: List[Dict[str, str]] = []
        for item in flatten_columns:
            if isinstance(item, str):
                path = item.strip()
                if path:
                    normalized.append({"path": path, "type": "VARCHAR"})
            elif isinstance(item, dict):
                path = str(item.get("path", "")).strip()
                dtype = str(item.get("type", "VARCHAR")).strip() or "VARCHAR"
                if path:
                    normalized.append({"path": path, "type": dtype})
        return normalized

    def _json_extract_expr(self, path: str, source_alias: str) -> str:
        """Build JSON path extraction expression from dotted config paths."""
        parts = [part.strip() for part in path.split(".") if part.strip()]
        if not parts:
            raise ValueError("Flatten path cannot be empty.")

        base_expr = f"{source_alias}.{parts[0]}"
        json_expr = f"TRY_PARSE_JSON(TO_VARCHAR({base_expr}))"

        for key in parts[1:]:
            escaped_key = key.replace('"', '""')
            json_expr = f'{json_expr}:"{escaped_key}"'
        return json_expr

    def get_table_columns(self, table_name: str, conn=None) -> List[str]:
        """Read physical column names so view generation can respect Snowflake casing."""
        database, schema, table = self._name_parts(table_name)
        info_schema = f"{self.quote_identifier(database)}.INFORMATION_SCHEMA.COLUMNS"
        sql = f"""
        SELECT COLUMN_NAME
        FROM {info_schema}
        WHERE UPPER(TABLE_SCHEMA) = UPPER(%s)
          AND UPPER(TABLE_NAME) = UPPER(%s)
        ORDER BY ORDINAL_POSITION
        """
        with self._connection(conn) as active_conn:
            cursor = active_conn.cursor()
            cursor.execute(sql, (schema, table))
            return [row[0] for row in cursor.fetchall()]

    @staticmethod
    def _normalize_unquoted_identifier(name: str) -> str:
        token = re.sub(r"[^A-Za-z0-9_]", "_", name.strip()).upper()
        token = token.strip("_")
        if not token:
            token = "COL"
        if token[0].isdigit():
            token = f"COL_{token}"
        return token

    def _build_projection_aliases(
        self, source_columns: Iterable[str]
    ) -> Dict[str, str]:
        """Create deterministic unquoted aliases for stable filter and flatten paths."""
        aliases: Dict[str, str] = {}
        used: set[str] = set()
        for col in source_columns:
            base = self._normalize_unquoted_identifier(col)
            candidate = base
            idx = 2
            while candidate in used:
                candidate = f"{base}_{idx}"
                idx += 1
            used.add(candidate)
            aliases[col] = candidate
        return aliases
