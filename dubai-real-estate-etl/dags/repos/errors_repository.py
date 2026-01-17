import json
from psycopg2.extensions import connection as Connection


class ErrorsRepository:
    """
    Repository responsible for writing ETL errors to stg.load_errors.
    """

    def insert_error(
        self,
        conn: Connection,
        workflow_key: str,
        record: dict,
        error_message: str,
        error_type: str,
        ingestion_id,
        stacktrace: str = None,
    ) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stg.load_errors(
                    workflow_key,
                    source_record,
                    error_message,
                    error_type,
                    ingestion_id,
                    stacktrace
                )
                VALUES (%(workflow_key)s,
                        %(source_record)s,
                        %(error_message)s,
                        %(error_type)s,
                        %(ingestion_id)s,
                        %(stacktrace)s);
                """,
                {
                    "workflow_key": workflow_key,
                    "source_record": json.dumps(record) if record else None,
                    "error_message": error_message,
                    "error_type": error_type,
                    "ingestion_id": ingestion_id,
                    "stacktrace": stacktrace,
                },
            )