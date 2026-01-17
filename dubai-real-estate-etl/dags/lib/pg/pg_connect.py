from contextlib import contextmanager
from airflow.providers.postgres.hooks.postgres import PostgresHook


class PgConnect:
    def __init__(self, conn_id: str) -> None:
        self.conn_id = conn_id

    @contextmanager
    def connection(self):
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        conn = hook.get_conn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
