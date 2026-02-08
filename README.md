# Snowflake ETL with Prefect and MinIO (Local Challenge Project)

This project implements the Senior Data Engineer code challenge:

- CSV ingestion
- Type inference (`NUMBER`, `FLOAT`, `VARCHAR`, `DATE`, `VARIANT`)
- Staging-table load with incremental `MERGE` into a parent table
- Filtered subsets as Snowflake secure views with optional JSON flattening
- Local orchestration via Docker Compose (Prefect + MinIO)

## Architecture (High Level)

1. Upload CSV to MinIO.
2. Download the same CSV locally (for local-first testing).
3. Infer Snowflake column types.
4. Create parent table if needed.
5. Create internal Snowflake stage.
6. `PUT` local CSV to stage.
7. `COPY INTO` temporary staging table.
8. `MERGE` staging data into parent table by `id`.
9. Run optional validations and create subset secure views.

## Prerequisites

- Docker + Docker Compose
- Python 3.12+
- `uv`
- Snowflake account

## 1) Configure Environment

Copy the template and fill credentials:

```bash
cp .env.example .env
```

Required groups:

- Snowflake connection values
- MinIO values (`MINIO_ENDPOINT`, access/secret keys, bucket)
- Prefect server values used by Docker Compose

## 2) One-Time Snowflake Setup

Run in a Snowflake workspace (if you like to adjust names make sure they correspond with the ones in the .env file):

```sql
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH WITH WAREHOUSE_SIZE = 'XSMALL';
CREATE DATABASE IF NOT EXISTS ETL_DB;
CREATE SCHEMA IF NOT EXISTS ETL_DB.PUBLIC;
```

Make sure database is created succesffuly

```sql
SHOW DATABASES;
```

## 3) Start Local Services

```bash
docker compose up -d
uv venv
source .venv/bin/activate
uv sync
```

Services:

- Prefect UI: `http://localhost:4200`
- MinIO API: `http://localhost:9000`
- MinIO Console: `http://localhost:9001`

Note: Flows in the Prefect UI will start to show up after they are served in the first run.

## 4) Run the ETL Flow

```bash
uv run python main.py --csv data/sample/events_1.csv
```

Optional flags:

- `--table <name>` target table (default: `events`)
- `--no-subsets` skip subset view creation

## 5) Run Subset Flow Separately (Optional)

```bash
uv run python -m flows.subset
```

Subset config file:

- `./config/subsets.yml`

Example:

```yaml
source_table: events
filters:
  - name: germany_events
    where: "country = 'DE'"
    flatten:
      - path: "event_metadata.user_id"
        type: "NUMBER"
  - name: recent_signups
    where: "event_type = 'signup'"
```

Note: `where` clauses are treated as trusted internal config and are injected directly into view SQL.

## CSV Requirements

Expected shape:

```csv
id,name,country,event_type,event_date,event_metadata
1,John,DE,signup,2025-01-01,"{\"user_id\": 123, \"session_duration\": 45}"
2,Maria,FR,purchase,2025-01-02,"{\"user_id\": 456, \"amount\": 120.5}"
```

Note: To make it easy for testing, 3 sample csv files are already included in ./data/sample/

Rules:

- `id` is required as the merge key.
- JSON object/array-like values are inferred as `VARIANT`.
- Date-like `...date...` columns are inferred as `DATE`.

## Verify Incremental MERGE Behavior

1. Initial load:

```bash
uv run python main.py --csv data/sample/events_1.csv
```

2. Re-run with another file containing new and existing IDs:

```bash
uv run python main.py --csv data/sample/events_3.csv
```

3. Validate in Snowflake:

```sql
SELECT COUNT(*) FROM ETL_DB.PUBLIC.EVENTS;
SELECT * FROM ETL_DB.PUBLIC.EVENTS ORDER BY ID;
SELECT * FROM ETL_DB.PUBLIC."germany_events" LIMIT 20;
SELECT * FROM ETL_DB.PUBLIC."recent_signups" LIMIT 20;
```

Expected:

- New IDs are inserted.
- Existing IDs are updated only when non-key fields changed.
- No delete handling (by design for this challenge).

## Validation Included

- Minimum row-count check after load
- Sample rows printed from Snowflake

## Tests

Run tests:

```bash
uv run pytest
```

Coverage includes:

- Type inference (including `VARIANT` and date formats)
- SQL generation and merge predicates
- Subset config validation
- Pipeline smoke path with mocked dependencies

## Troubleshooting

- MinIO auth/bucket issues: verify `MINIO_ROOT_*`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_BUCKET`.
- Snowflake auth issues: verify `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, role privileges.
- Prefect not reachable: verify `docker compose ps` and `PREFECT_API_URL=http://localhost:4200/api`.
