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

class DatabaseSink:
    """Write batches of raw API records to a fresh database table."""

    def __init__(self, db_url: str, table_name: str):
        self.db_url = db_url
        self.table_name = table_name
        self.rows_written = 0
        self.engine = create_engine(db_url)
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
            self.table_name,
            self.engine,
            if_exists="append",
            index=False,
        )

        self.rows_written += len(page_df)
 
    def start_fresh_load(self, programme):
        """Start a new staging load for one programme."""
        with self.engine.begin() as connection:
            connection.execute(
                text(f'DROP TABLE IF EXISTS "{self.table_name}"')
            )

        self.programme = programme
        self.rows_written = 0
        self.page_failures = []
        self.fetched_at = datetime.now(timezone.utc)

    def count_rows(self, table_name: str | None = None) -> int:
        """Return the number of rows in a table, or zero when it does not exist."""
        table_name = table_name or self.table_name

        if not inspect(self.engine).has_table(table_name):
            return 0

        with self.engine.connect() as connection:
            return connection.execute(
                text(f'SELECT COUNT(*) FROM "{table_name}"')
            ).scalar_one()

    def count_programme_rows(
        self,
        programme_id: int,
        table_name: str | None = None,
    ) -> int:
        """Return the number of rows stored for one programme."""
        table_name = table_name or self.table_name

        if not inspect(self.engine).has_table(table_name):
            return 0

        with self.engine.connect() as connection:
            return connection.execute(
                text(
                    f'SELECT COUNT(*) FROM "{table_name}" '
                    "WHERE programme_id = :programme_id"
                ),
                {"programme_id": programme_id},
            ).scalar_one()

    def publish_programme_staging(
        self,
        staging_table: str,
        published_table: str,
    ) -> dict[str, int]:
        """Insert new projects and refresh existing projects from staging."""
        inspector = inspect(self.engine)
        programme_id = self.programme.id

        # Nothing was fetched for this programme, so there is nothing to publish.
        if not inspector.has_table(staging_table):
            return {
                "staged": 0,
                "invalid": 0,
                "duplicate_staged": 0,
                "inserted": 0,
                "updated": 0,
                "published_for_programme":
                    self.count_programme_rows(programme_id, published_table),
            }

        with self.engine.begin() as connection:
            # On the first publication, create an empty published table with the
            # same schema as the staging table.
            if not inspector.has_table(published_table):
                connection.execute(text(
                    f'CREATE TABLE "{published_table}" AS '
                    f'SELECT * FROM "{staging_table}" WHERE 1 = 0'
                ))

            # ON CONFLICT requires reference to be unique. This also makes
            # lookups and conflict detection efficient.
            #
            # Use a new index name because the database may already contain the
            # older, non-unique ix_<table>_reference index.
            connection.execute(text(
                f'CREATE UNIQUE INDEX IF NOT EXISTS '
                f'"ux_{published_table}_reference" '
                f'ON "{published_table}" ("reference")'
            ))

            connection.execute(text(
                f'CREATE INDEX IF NOT EXISTS '
                f'"ix_{published_table}_programme_id" '
                f'ON "{published_table}" ("programme_id")'
            ))

            # Count every staged row, including invalid and duplicate rows.
            staged = connection.execute(
                text(f"""
                    SELECT COUNT(*)
                    FROM "{staging_table}"
                    WHERE programme_id = :programme_id
                """),
                {"programme_id": programme_id},
            ).scalar_one()

            # Rows without a usable reference cannot be safely published.
            invalid = connection.execute(
                text(f"""
                    SELECT COUNT(*)
                    FROM "{staging_table}"
                    WHERE programme_id = :programme_id
                    AND (reference IS NULL OR TRIM(reference) = '')
                """),
                {"programme_id": programme_id},
            ).scalar_one()

            # This is the number of distinct, publishable projects in staging.
            distinct_valid = connection.execute(
                text(f"""
                    SELECT COUNT(DISTINCT reference)
                    FROM "{staging_table}"
                    WHERE programme_id = :programme_id
                    AND reference IS NOT NULL
                    AND TRIM(reference) <> ''
                """),
                {"programme_id": programme_id},
            ).scalar_one()

            # Count staged references that are already published. These rows will
            # be refreshed rather than inserted.
            existing = connection.execute(
                text(f"""
                    SELECT COUNT(DISTINCT staged.reference)
                    FROM "{staging_table}" AS staged
                    INNER JOIN "{published_table}" AS published
                        ON published.reference = staged.reference
                    WHERE staged.programme_id = :programme_id
                    AND staged.reference IS NOT NULL
                    AND TRIM(staged.reference) <> ''
                """),
                {"programme_id": programme_id},
            ).scalar_one()

            # Read the staging schema so the statement continues to work if the
            # API supplies additional project columns in future.
            columns = [
                column["name"]
                for column in inspector.get_columns(staging_table)
            ]
            quoted_columns = ", ".join(
                f'"{column}"' for column in columns
            )

            # The reference identifies the row and must not be changed. Every
            # other column is refreshed from the newly fetched staged row.
            update_columns = [
                column for column in columns
                if column != "reference"
            ]
            update_assignments = ", ".join(
                f'"{column}" = EXCLUDED."{column}"'
                for column in update_columns
            )

            # Deduplicate staging by reference, then:
            #
            # - insert references that are not yet published;
            # - update every non-key field for references already published;
            # - leave published projects absent from staging untouched.
            connection.execute(
                text(f"""
                    INSERT INTO "{published_table}" ({quoted_columns})
                    SELECT {quoted_columns}
                    FROM (
                        SELECT *,
                            ROW_NUMBER() OVER (
                                PARTITION BY reference
                                ORDER BY reference
                            ) AS row_number
                        FROM "{staging_table}"
                        WHERE programme_id = :programme_id
                        AND reference IS NOT NULL
                        AND TRIM(reference) <> ''
                    ) AS staged
                    WHERE row_number = 1
                    ON CONFLICT (reference) DO UPDATE
                    SET {update_assignments}
                """),
                {"programme_id": programme_id},
            )

            published_after = connection.execute(
                text(f"""
                    SELECT COUNT(*)
                    FROM "{published_table}"
                    WHERE programme_id = :programme_id
                """),
                {"programme_id": programme_id},
            ).scalar_one()

        return {
            "staged": staged,
            "invalid": invalid,
            "duplicate_staged": staged - invalid - distinct_valid,
            "inserted": distinct_valid - existing,
            "updated": existing,
            "published": published_after,
        }      