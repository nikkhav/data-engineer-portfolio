from lib.pg import PgConnect
import uuid


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def insert_user_category_counters(
            self,
            user_id: uuid.UUID,
            category_id: uuid.UUID,
            category_name: str,
            order_cnt: int,
    ) -> None:
        sql_insert = f"""
                    INSERT INTO cdm.user_category_counters(user_id, category_id, category_name, order_cnt)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (user_id, category_id) DO UPDATE
                    SET order_cnt = cdm.user_category_counters.order_cnt + 1;
                """

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_insert, (
                    user_id,
                    category_id,
                    category_name,
                    order_cnt
                ))

    def insert_user_product_counters(
            self,
            user_id: uuid.UUID,
            product_id: uuid.UUID,
            product_name: str,
            order_cnt: int,
    ) -> None:
        sql_insert = f"""
                    INSERT INTO cdm.user_product_counters(user_id, product_id, product_name, order_cnt)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (user_id, product_id) DO UPDATE
                    SET order_cnt = cdm.user_product_counters.order_cnt + 1;
                """

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_insert, (
                    user_id,
                    product_id,
                    product_name,
                    order_cnt
                ))
