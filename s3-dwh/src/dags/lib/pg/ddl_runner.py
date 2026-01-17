import os
from logging import Logger
from pathlib import Path

from lib.pg.pg_connect import PgConnect


class DdlRunner:
    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self._db = pg
        self.log = log

    def run(self, path_to_scripts: str) -> None:
        files = os.listdir(path_to_scripts)
        file_paths = [Path(path_to_scripts, f) for f in files]
        file_paths.sort(key=lambda x: x.name)

        self.log.info(f"Found {len(file_paths)} DDL files.")

        for i, fp in enumerate(file_paths, start=1):
            self.log.info(f"[DDL] Step {i}: applying {fp.name}")
            script = fp.read_text()

            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(script)

            self.log.info(f"[DDL] Step {i}: {fp.name} executed successfully.")
