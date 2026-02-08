from pathlib import Path
from typing import Any

from prefect import flow
import yaml

from config.settings import SnowflakeSettings
from utils.snowflake_helpers import SnowflakeHelper


def _validate_subsets_config(
    config: dict[str, Any],
) -> tuple[str, list[dict[str, Any]]]:
    """Validate subset config shape early so SQL generation can stay deterministic."""
    if not isinstance(config, dict):
        raise ValueError("Subset config must be a YAML object.")

    source_table = config.get("source_table", "events")
    if not isinstance(source_table, str) or not source_table.strip():
        raise ValueError("`source_table` must be a non-empty string.")

    filters = config.get("filters", [])
    if not isinstance(filters, list):
        raise ValueError("`filters` must be a list.")

    validated_filters: list[dict[str, Any]] = []
    for idx, filter_def in enumerate(filters):
        if not isinstance(filter_def, dict):
            raise ValueError(f"filters[{idx}] must be an object.")

        name = filter_def.get("name")
        where = filter_def.get("where")
        flatten = filter_def.get("flatten", [])

        if not isinstance(name, str) or not name.strip():
            raise ValueError(f"filters[{idx}].name must be a non-empty string.")
        if not isinstance(where, str) or not where.strip():
            raise ValueError(f"filters[{idx}].where must be a non-empty string.")

        if flatten is None:
            flatten = []
        if not isinstance(flatten, list):
            raise ValueError(f"filters[{idx}].flatten must be a list when provided.")

        for jdx, item in enumerate(flatten):
            if isinstance(item, str):
                if not item.strip():
                    raise ValueError(f"filters[{idx}].flatten[{jdx}] cannot be empty.")
                continue
            if isinstance(item, dict):
                path = item.get("path")
                dtype = item.get("type", "VARCHAR")
                if not isinstance(path, str) or not path.strip():
                    raise ValueError(
                        f"filters[{idx}].flatten[{jdx}].path must be a non-empty string."
                    )
                if dtype is not None and not isinstance(dtype, str):
                    raise ValueError(
                        f"filters[{idx}].flatten[{jdx}].type must be a string when provided."
                    )
                continue
            raise ValueError(
                f"filters[{idx}].flatten[{jdx}] must be a string or an object."
            )

        validated_filters.append(
            {
                "name": name.strip(),
                "where": where.strip(),
                "flatten": flatten,
            }
        )

    return source_table.strip(), validated_filters


@flow(name="create-subsets")
def create_subsets():
    """
    Materialize configured secure views from validated subset definitions.

    Trust boundary: `where` clauses are treated as trusted internal SQL after
    shape validation. This function does not sanitize SQL expressions.
    """

    sf_cfg = SnowflakeSettings()
    helper = SnowflakeHelper(sf_cfg)

    config_path = Path("config/subsets.yml")
    config = yaml.safe_load(config_path.read_text()) or {}
    source_table, filters = _validate_subsets_config(config)
    created = []

    for filter_def in filters:
        helper.create_secure_view(
            view_name=filter_def["name"],
            source_table=source_table,
            where_clause=filter_def["where"],
            flatten_columns=filter_def.get("flatten"),
        )

        print(f"View created: {filter_def['name']}")
        created.append(filter_def["name"])

    return created


if __name__ == "__main__":
    create_subsets()
