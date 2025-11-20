import uuid
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    # --- HUBS ---
    HUB_CONFIG = {
        'h_user': {
            'pk': 'h_user_pk',
            'bk': 'user_id',
            'extra': []
        },
        'h_product': {
            'pk': 'h_product_pk',
            'bk': 'product_id',
            'extra': []
        },
        'h_restaurant': {
            'pk': 'h_restaurant_pk',
            'bk': 'restaurant_id',
            'extra': []
        },
        'h_category': {
            'pk': 'h_category_pk',
            'bk': 'category_name',
            'extra': []
        },
        'h_order': {
            'pk': 'h_order_pk',
            'bk': 'order_id',
            'extra': ['order_dt']
        }
    }

    # По требованию заказчика для h_order требуется дополнительный атрибут order_dt
    # поэтому метод insert_hub принимает **extra для передачи дополнительных колонок
    def insert_hub(self, hub_table: str, bk_value: Any, load_src: str, **extra):
        cfg = self.HUB_CONFIG[hub_table]

        pk_col = cfg['pk']
        bk_col = cfg['bk']
        extra_cols = cfg['extra']

        hub_pk = uuid.uuid5(uuid.NAMESPACE_OID, str(bk_value))

        cols = [pk_col, bk_col, 'load_src']
        vals = [hub_pk, bk_value, load_src]

        for col in extra_cols:
            cols.append(col)
            vals.append(extra[col])

        columns = ', '.join(cols)
        placeholders = ', '.join(['%s'] * len(vals))

        sql = f"""
            INSERT INTO dds.{hub_table} ({columns})
            VALUES ({placeholders})
            ON CONFLICT ({pk_col}) DO NOTHING;
        """

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, vals)
        return hub_pk

    # --- LINKS ---
    LINK_PK = {
        'l_order_product': 'hk_order_product_pk',
        'l_product_category': 'hk_product_category_pk',
        'l_product_restaurant': 'hk_product_restaurant_pk',
        'l_order_user': 'hk_order_user_pk'
    }

    def insert_link(self, link_table: str, hub_keys: Dict[str, uuid.UUID], load_src: str) -> uuid.UUID:
        link_pk = uuid.uuid5(uuid.NAMESPACE_OID, ''.join(str(v) for v in hub_keys.values()))
        pk_col = self.LINK_PK[link_table]

        columns = ', '.join(hub_keys.keys())
        placeholders = ', '.join(['%s'] * len(hub_keys))

        sql = f"""
              INSERT INTO dds.{link_table} ({pk_col}, {columns}, load_src)
              VALUES (%s, {placeholders}, %s)
              ON CONFLICT ({pk_col}) DO NOTHING;
              """

        params = [link_pk] + list(hub_keys.values()) + [load_src]

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
        return link_pk

    # --- SATELLITES ---
    HASHDIFF_COLUMNS = {
        's_order_cost': 'hk_order_cost_hashdiff',
        's_order_status': 'hk_order_status_hashdiff',
        's_user_names': 'hk_user_names_hashdiff',
        's_product_names': 'hk_product_names_hashdiff',
        's_restaurant_names': 'hk_restaurant_names_hashdiff'
    }

    SAT_PK = {
        's_order_cost': 'h_order_pk',
        's_order_status': 'h_order_pk',
        's_user_names': 'h_user_pk',
        's_product_names': 'h_product_pk',
        's_restaurant_names': 'h_restaurant_pk'
    }

    def insert_satellite(self, sat_table: str, hub_pk: uuid.UUID, payload: Dict[str, Any], load_src: str):
        ordered_payload = {k: payload[k] for k in sorted(payload.keys())}

        payload_str = '|'.join(str(v) for v in ordered_payload.values())
        hashdiff = uuid.uuid5(uuid.NAMESPACE_OID, payload_str)

        hashdiff_col = self.HASHDIFF_COLUMNS[sat_table]
        hub_pk_col = self.SAT_PK[sat_table]

        sql_check = f"""
            SELECT {hashdiff_col}
            FROM dds.{sat_table}
            WHERE {hub_pk_col} = %s
            ORDER BY load_dt DESC
            LIMIT 1;
        """

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_check, (hub_pk,))
                row = cur.fetchone()

        if row and row[0] == hashdiff:
            return

        columns = ', '.join(ordered_payload.keys())
        placeholders = ', '.join(['%s'] * len(ordered_payload))

        sql_insert = f"""
            INSERT INTO dds.{sat_table}
            ({hub_pk_col}, {columns}, {hashdiff_col}, load_src)
            VALUES (%s, {placeholders}, %s, %s);
        """

        params = [hub_pk] + list(ordered_payload.values()) + [hashdiff, load_src]
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_insert, params)
