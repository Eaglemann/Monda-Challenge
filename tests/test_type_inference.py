from pathlib import Path

import pandas as pd

from utils.type_inference import TypeInferenceEngine


SAMPLE_FILES = [
    Path("data/sample/events_1.csv"),
    Path("data/sample/events_2.csv"),
    Path("data/sample/events_3.csv"),
]


def test_event_metadata_inferred_as_variant_for_samples() -> None:
    engine = TypeInferenceEngine()
    for sample_path in SAMPLE_FILES:
        df = pd.read_csv(sample_path)
        inferred = engine.infer_types(df)
        assert inferred["event_metadata"] == "VARIANT"


def test_event_date_inferred_as_date_for_mixed_formats() -> None:
    engine = TypeInferenceEngine()
    df_iso = pd.read_csv("data/sample/events_1.csv")
    df_short = pd.read_csv("data/sample/events_3.csv")

    inferred_iso = engine.infer_types(df_iso)
    inferred_short = engine.infer_types(df_short)

    assert inferred_iso["event_date"] == "DATE"
    assert inferred_short["event_date"] == "DATE"


def test_regular_text_column_stays_varchar() -> None:
    engine = TypeInferenceEngine()
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Ana", "Luca", "Carlos"],
            "event_metadata": ['{"user_id": 1}', '{"user_id": 2}', '{"user_id": 3}'],
        }
    )
    inferred = engine.infer_types(df)
    assert inferred["name"] == "VARCHAR"
