import argparse

from flows.pipeline import etl_pipeline
from flows.subset import create_subsets
from utils.validators import summarize_run


def main() -> None:
    parser = argparse.ArgumentParser(description="Run ETL pipeline")
    parser.add_argument("--csv", required=True, help="Path to CSV file")
    parser.add_argument("--table", default="events", help="Target table name")
    parser.add_argument(
        "--run-subsets",
        dest="run_subsets",
        action="store_true",
        help="Create subset secure views after load (default)",
    )
    parser.add_argument(
        "--no-subsets",
        dest="run_subsets",
        action="store_false",
        help="Skip subset creation",
    )
    parser.set_defaults(run_subsets=True)
    args = parser.parse_args()

    result = etl_pipeline(
        args.csv,
        table_name=args.table,
    )

    views = []
    if args.run_subsets:
        views = create_subsets()

    summarize_run(
        input_file=result.get("input_file"),
        object_name=result.get("object_name"),
        table=result.get("table"),
        rows=result.get("rows"),
        views=views,
        validation_passed=result.get("validation_passed", False),
        inserted=result.get("inserted"),
        updated=result.get("updated"),
        column_count=result.get("column_count"),
    )


if __name__ == "__main__":
    main()
