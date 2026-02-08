import pytest

from flows.subset import _validate_subsets_config


def test_subset_config_accepts_string_and_object_flatten_formats() -> None:
    source_table, filters = _validate_subsets_config(
        {
            "source_table": "events",
            "filters": [
                {
                    "name": "germany_events",
                    "where": "country = 'DE'",
                    "flatten": ["event_metadata.user_id"],
                },
                {
                    "name": "recent_signups",
                    "where": "event_type = 'signup'",
                    "flatten": [
                        {"path": "event_metadata.session_duration", "type": "NUMBER"}
                    ],
                },
            ],
        }
    )

    assert source_table == "events"
    assert len(filters) == 2


def test_subset_config_rejects_missing_where_clause() -> None:
    with pytest.raises(ValueError, match=r"filters\[0\]\.where"):
        _validate_subsets_config(
            {
                "source_table": "events",
                "filters": [{"name": "bad_filter"}],
            }
        )
