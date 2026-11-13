import json
from sqlalchemy import create_engine, inspect, text
import pandas as pd
from datetime import datetime, timezone


def sql_safe_df(df: pd.DataFrame) -> pd.DataFrame:
    """Convert nested API values into JSON strings sql can store."""
    return df.map(
        lambda value: json.dumps(value)
        if isinstance(value, (dict, list))
        else value
    )

def _execute(connection, query, params=None): return connection.execute(text(query), params)

class DatabaseSink:
    """Write batches of raw API records to a fresh database table."""

    def __init__(self, db_url: str, staging_table: str):
        "Create a reusable sink; programme state is set by start_fresh_load."
        self.db_url,self.staging_table = db_url,staging_table
        self.engine = create_engine(db_url)
        self.programme,self.fetched_at = None,None
        self.rows_written = 0
        self.page_failures = []

    def write_programme_batch(self, page_df):
        """Write one transformed page with programme provenance."""
        if page_df.empty:
            return

        page_df = page_df.assign(
            programme_id=self.programme.id,
            programme_name=self.programme.name,
            fetched_at=self.fetched_at
        )

        sql_safe_df(page_df).to_sql(
            self.staging_table,
            self.engine,
            if_exists="append",
            index=False,
        )

        self.rows_written += len(page_df)

    def start_fresh_load(self, programme):
        """Start a new staging load for one programme."""
        with self.engine.begin() as connection:
            connection.execute(
                text(f'DROP TABLE IF EXISTS "{self.staging_table}"')
            )

        self.programme = programme
        self.rows_written = 0
        self.page_failures = []
        self.fetched_at = datetime.now(timezone.utc)

    def count_rows(self, table_name: str | None = None, programme_id: int | None = None) -> int:
        "Count rows, optionally for one programme; return zero if the table is absent."
        table_name = table_name or self.staging_table
        if not inspect(self.engine).has_table(table_name): return 0

        query = f'SELECT COUNT(*) FROM "{table_name}"'
        params = {}
        if programme_id is not None:
            query += " WHERE programme_id = :programme_id"
            params = dict(programme_id=programme_id)

        with self.engine.connect() as connection:
            return connection.execute(text(query), params).scalar_one()

    def _prepare_published(self, connection, inspector, staging_table, published_table):
        "Ensure the published table and publication indexes exist."
        create_sql = f'CREATE TABLE "{published_table}" AS SELECT * FROM "{staging_table}" WHERE 1 = 0'
        if not inspector.has_table(published_table): _execute(connection, create_sql)

        _execute(connection,
            f'CREATE UNIQUE INDEX IF NOT EXISTS "ux_{published_table}_reference" '
            f'ON "{published_table}" ("reference")')
        _execute(connection,
            f'CREATE INDEX IF NOT EXISTS "ix_{published_table}_programme_id" '
            f'ON "{published_table}" ("programme_id")')

    def _publication_counts(self, connection, staging_table, published_table, programme_id):
        "Count staged rows, invalid references, distinct valid references, and existing matches."
        params = dict(programme_id=programme_id)
        programme_filter = 'programme_id = :programme_id'
        valid_reference = "reference IS NOT NULL AND TRIM(reference) <> ''"

        staged = _execute(connection,
            f'SELECT COUNT(*) FROM "{staging_table}" WHERE {programme_filter}', params).scalar_one()
        invalid = _execute(connection,
            f'''SELECT COUNT(*) FROM "{staging_table}"
                WHERE {programme_filter} AND (reference IS NULL OR TRIM(reference) = '')''', params).scalar_one()
        distinct_valid = _execute(connection,
            f'''SELECT COUNT(DISTINCT reference) FROM "{staging_table}"
                WHERE {programme_filter} AND {valid_reference}''', params).scalar_one()
        existing = _execute(connection,
            f'''SELECT COUNT(DISTINCT staged.reference)
                FROM "{staging_table}" AS staged
                INNER JOIN "{published_table}" AS published ON published.reference = staged.reference
                WHERE staged.programme_id = :programme_id
                AND staged.reference IS NOT NULL AND TRIM(staged.reference) <> '' ''', params).scalar_one()

        return staged,invalid,distinct_valid,existing

    def _upsert_staging(self, connection, inspector, staging_table, published_table, programme_id):
        "Insert valid staged references and refresh every non-key field on existing matches."
        columns = [col["name"] for col in inspector.get_columns(staging_table)]
        quoted_columns = ", ".join(f'"{col}"' for col in columns)
        update_assignments = ", ".join(f'"{col}" = EXCLUDED."{col}"' for col in columns if col != "reference")

        update_query = f'''WITH ranked_staging AS (
                            SELECT *,
                                -- Number rows within each reference group.
                                -- This does not choose the newest version.
                                ROW_NUMBER() OVER (
                                    PARTITION BY reference
                                    ORDER BY reference
                                ) AS row_number
                            FROM "{staging_table}"
                            WHERE programme_id = :programme_id
                                AND reference IS NOT NULL
                                AND TRIM(reference) <> ''
                            )

                            -- Attempt to insert one valid staged row per reference.
                            INSERT INTO "{published_table}" ({quoted_columns})
                            SELECT {quoted_columns}
                            FROM ranked_staging
                            WHERE row_number = 1

                            -- If the reference already exists, refresh its non-key columns.
                            ON CONFLICT (reference) DO UPDATE
                            SET {update_assignments}'''

        _execute(connection, update_query, dict(programme_id=programme_id))

    def publish_programme_staging(self, staging_table: str, published_table: str) -> dict[str, int]:
        """Insert new projects and refresh existing projects from staging."""
        inspector = inspect(self.engine)
        programme_id = self.programme.id

        # If nothing was fetched for this programme, there is nothing to publish.
        if not inspector.has_table(staging_table):
            return {
                "staged": 0,
                "invalid": 0,
                "duplicate_staged": 0,
                "inserted": 0,
                "updated": 0,
                "published":
                    self.count_rows(table_name=published_table, programme_id=programme_id),
            }

        with self.engine.begin() as connection:
            self._prepare_published(connection, inspector, staging_table, published_table)

            staged,invalid,distinct_valid,existing = self._publication_counts(
                connection, staging_table, published_table, programme_id)

            self._upsert_staging(connection, inspector, staging_table, published_table, programme_id)

            published_after = _execute(connection,
                f'SELECT COUNT(*) FROM "{published_table}" WHERE programme_id = :programme_id',
                dict(programme_id=programme_id)).scalar_one()

        return {
            "staged": staged,
            "invalid": invalid,
            "duplicate_staged": staged - invalid - distinct_valid,
            "inserted": distinct_valid - existing,
            "updated": existing,
            "published": published_after,
        }
