from typing import Dict, Any
import json

import pandas as pd


class TypeInferenceEngine:
    """
    Infer Snowflake column types from CSV samples with conservative heuristics.

    Thresholds trade off false positives and false negatives:
    - higher thresholds avoid accidental JSON/DATE typing
    - lower thresholds detect sparse semi-structured/date columns earlier
    """

    SNOWFLAKE_TYPE_MAP = {
        "int64": "NUMBER",
        "Int64": "NUMBER",
        "float64": "FLOAT",
        "Float64": "FLOAT",
        "bool": "BOOLEAN",
        "boolean": "BOOLEAN",
        "datetime64[ns]": "TIMESTAMP_NTZ",
        "object": "VARCHAR",
        "string": "VARCHAR",
        "str": "VARCHAR",
    }

    def __init__(
        self,
        json_threshold: float = 0.6,
        date_threshold: float = 0.8,
        sample_size: int = 20,
    ) -> None:
        self.json_threshold = json_threshold
        self.date_threshold = date_threshold
        self.sample_size = sample_size

    def infer_types(self, df: pd.DataFrame) -> Dict[str, str]:
        """Infer Snowflake types for each column in a DataFrame sample."""
        type_map = {}
        for col in df.columns:
            type_map[col] = self._infer_column_type(df[col], col)
        return type_map

    def _infer_column_type(self, series: pd.Series, col_name: str) -> str:
        """Resolve a single column to Snowflake type with JSON/DATE precedence."""
        if self._is_json_column(series):
            return "VARIANT"
        if self._is_date_column(series, col_name):
            return "DATE"
        return self.SNOWFLAKE_TYPE_MAP.get(str(series.dtype), "VARCHAR")

    def _is_json_column(self, series: pd.Series) -> bool:
        """
        Detect whether values should be treated as semi-structured JSON payloads.
        """
        sample = series.dropna().head(self.sample_size)
        if sample.empty:
            return False

        valid_json_values = 0
        checked_values = 0

        for val in sample:
            if isinstance(val, (dict, list)):
                checked_values += 1
                valid_json_values += 1
                continue

            if not isinstance(val, str):
                continue

            stripped = val.strip()
            if not stripped:
                continue

            checked_values += 1
            if not (stripped.startswith("{") or stripped.startswith("[")):
                continue

            try:
                parsed: Any = json.loads(stripped)
            except (TypeError, json.JSONDecodeError):
                continue

            # Restrict to object/array payloads; scalars like "1" remain VARCHAR.
            if isinstance(parsed, (dict, list)):
                valid_json_values += 1

        if checked_values == 0:
            return False

        return (valid_json_values / checked_values) >= self.json_threshold

    def _is_date_column(self, series: pd.Series, col_name: str) -> bool:
        """Classify date columns using name hint + parse ratio to reduce mis-typing."""
        if "date" not in col_name.lower():
            return False

        sample = series.dropna().head(self.sample_size)
        if sample.empty:
            return False

        parsed = pd.to_datetime(sample.astype(str), errors="coerce", format="mixed")
        valid_ratio = parsed.notna().sum() / len(sample)
        return valid_ratio >= self.date_threshold
