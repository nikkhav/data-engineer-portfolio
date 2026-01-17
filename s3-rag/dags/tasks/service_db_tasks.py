from __future__ import annotations

import psycopg2
import logging


def save_service_records(files: list[dict]):
    if not files:
        logging.info("Nothing to save")
        return

    # Use env variables in production
    pg = psycopg2.connect(
        host="postgres",
        port=5432,
        user="jovyan",
        password="jovyan",
        dbname="service_s3",
    )

    cursor = pg.cursor()

    for f in files:
        cursor.execute(
            """
            INSERT INTO document_ingestion
                (bucket_name, object_key, etag, status)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (bucket_name, object_key, etag)
            DO UPDATE SET
                status = EXCLUDED.status,
                updated_at = now()
            """,
            (f["bucket"], f["key"], f["etag"], "vectorized"),
        )

    pg.commit()
    cursor.close()
    pg.close()

    logging.info("Service records updated")
