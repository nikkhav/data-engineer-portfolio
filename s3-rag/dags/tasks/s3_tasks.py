from __future__ import annotations

import boto3
import psycopg2
import logging

BUCKET_NAME = "uploads"


def list_new_s3_objects() -> list[dict]:
    logging.info("Listing new S3 objects")

    # Use env variables in production
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )

    # Use env variables in production
    pg = psycopg2.connect(
        host="postgres",
        port=5432,
        user="jovyan",
        password="jovyan",
        dbname="service_s3",
    )

    cursor = pg.cursor()

    response = s3.list_objects_v2(Bucket=BUCKET_NAME)
    if "Contents" not in response:
        return []

    new_files = []

    for obj in response["Contents"]:
        cursor.execute(
            """
            SELECT 1 FROM document_ingestion
            WHERE bucket_name = %s AND object_key = %s AND etag = %s
            """,
            (BUCKET_NAME, obj["Key"], obj["ETag"]),
        )

        if cursor.fetchone() is None:
            new_files.append(
                {
                    "bucket": BUCKET_NAME,
                    "key": obj["Key"],
                    "etag": obj["ETag"],
                }
            )

    cursor.close()
    pg.close()

    logging.info(f"Found {len(new_files)} new files")
    return new_files
