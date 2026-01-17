import csv
from io import StringIO
from typing import Iterable

from psycopg2.extensions import connection as Connection

from .deal_obj import DealObj


class DealsDestRepository:
    """
    Repository responsible for inserting DealObj records into stg.raw_deals
    with efficient batch loading.
    """

    def insert_batch(self, conn: Connection, deals: Iterable[DealObj]) -> None:
        deals = list(deals)
        if not deals:
            return

        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TEMP TABLE tmp_raw_deals (
                    transaction_number VARCHAR NOT NULL,
                    payload JSONB NOT NULL,
                    load_source VARCHAR NOT NULL,
                    ingestion_id UUID NOT NULL
                ) ON COMMIT DROP;
                """
            )

            buffer = StringIO()
            writer = csv.writer(buffer)

            for d in deals:
                writer.writerow(
                    [
                        d.transaction_number,
                        d.payload,
                        "origin",
                        str(d.ingestion_id),
                    ]
                )

            buffer.seek(0)

            cur.copy_expert(
                """
                COPY tmp_raw_deals (
                    transaction_number,
                    payload,
                    load_source,
                    ingestion_id
                )
                FROM STDIN
                WITH (FORMAT CSV)
                """,
                buffer,
            )

            cur.execute(
                """
                INSERT INTO stg.raw_deals AS t (
                    transaction_number,
                    payload,
                    load_source,
                    ingestion_id
                )
                SELECT DISTINCT ON (transaction_number)
                    transaction_number,
                    payload,
                    load_source,
                    ingestion_id
                FROM tmp_raw_deals
                ORDER BY transaction_number, ingestion_id DESC
                ON CONFLICT (transaction_number) DO UPDATE
                SET payload      = EXCLUDED.payload,
                    load_source  = EXCLUDED.load_source,
                    ingestion_id = EXCLUDED.ingestion_id;
                """
            )
