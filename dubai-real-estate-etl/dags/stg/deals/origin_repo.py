import re
import time
import uuid
from datetime import datetime, UTC
from io import StringIO
from typing import List

import pandas as pd
import requests

from .deal_obj import DealObj


MAX_RETRIES = 5
RETRY_DELAY = 5


class DealsOriginRepository:
    """
    Extracts deal data from the external API (Dubai Land Department).

    Responsibilities:
      - Build request body
      - Send API requests with retry & backoff
      - Normalize column names (remove BOM / exotic chars)
      - Convert raw rows into DealObj models
      - Handle pagination via P_SKIP / P_TAKE
    """

    def __init__(self, url: str, batch_limit: int = 100):
        self.url = url
        self.batch_limit = batch_limit

    # Safe request with retries
    def _safe_post(self, body: dict) -> requests.Response:
        for attempt in range(MAX_RETRIES):
            try:
                res = requests.post(
                    self.url,
                    json=body,
                    headers={"Content-Type": "application/json"},
                    timeout=30,
                )
                res.raise_for_status()
                return res

            except Exception:
                if attempt == MAX_RETRIES - 1:
                    raise
                time.sleep(RETRY_DELAY * (attempt + 1))

        raise RuntimeError("Unexpected retry loop exit")

    # Build request body
    def _build_request_body(self, target_date: datetime, skip: int) -> dict:
        date_str = target_date.strftime("%m/%d/%Y")

        return {
            "parameters": {
                "P_FROM_DATE": date_str,
                "P_TO_DATE": date_str,
                "P_GROUP_ID": "",
                "P_IS_OFFPLAN": "",
                "P_IS_FREE_HOLD": "",
                "P_AREA_ID": "",
                "P_USAGE_ID": "",
                "P_PROP_TYPE_ID": "",
                "P_TAKE": str(self.batch_limit),
                "P_SKIP": str(skip),
                "P_SORT": "INSTANCE_DATE_ASC",
            },
            "command": "transactions",
            "labels": {
                "TRANSACTION_NUMBER": "TRANSACTION_NUMBER",
                "INSTANCE_DATE": "INSTANCE_DATE",
                "GROUP_EN": "GROUP_EN",
                "PROCEDURE_EN": "PROCEDURE_EN",
                "IS_OFFPLAN_EN": "IS_OFFPLAN_EN",
                "IS_FREE_HOLD_EN": "IS_FREE_HOLD_EN",
                "USAGE_EN": "USAGE_EN",
                "AREA_EN": "AREA_EN",
                "PROP_TYPE_EN": "PROP_TYPE_EN",
                "PROP_SB_TYPE_EN": "PROP_SB_TYPE_EN",
                "TRANS_VALUE": "TRANS_VALUE",
                "PROCEDURE_AREA": "PROCEDURE_AREA",
                "ACTUAL_AREA": "ACTUAL_AREA",
                "ROOMS_EN": "ROOMS_EN",
                "PARKING": "PARKING",
                "NEAREST_METRO_EN": "NEAREST_METRO_EN",
                "NEAREST_MALL_EN": "NEAREST_MALL_EN",
                "NEAREST_LANDMARK_EN": "NEAREST_LANDMARK_EN",
                "TOTAL_BUYER": "TOTAL_BUYER",
                "TOTAL_SELLER": "TOTAL_SELLER",
                "MASTER_PROJECT_EN": "MASTER_PROJECT_EN",
                "PROJECT_EN": "PROJECT_EN",
            },
        }

    # Clean column names from BOM/exotic chars
    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        for column in df.columns:
            clean = re.sub(r"[^0-9a-zA-Z.,\-_\/ ]", "", column)
            df.rename(columns={column: clean}, inplace=True)
        return df


    # Main method: Extract deals
    def list_deals(self, target_date: datetime, ingestion_id: uuid.UUID) -> List[DealObj]:
        skip = 0
        collected: List[DealObj] = []

        while True:
            body = self._build_request_body(target_date, skip)

            # API request with retry/backoff
            res = self._safe_post(body)

            df = pd.read_csv(StringIO(res.text))
            df = self._normalize_columns(df)

            # no rows → end of pagination
            if df.empty:
                break

            # transform into DealObj list
            for _, row in df.iterrows():
                collected.append(
                    DealObj(
                        transaction_number=row["TRANSACTION_NUMBER"],
                        instance_date=pd.to_datetime(row["INSTANCE_DATE"]),
                        payload=row.to_json(),
                        load_ts=datetime.now(UTC),
                        ingestion_id=ingestion_id,
                    )
                )

            # last page?
            if len(df) < self.batch_limit:
                break

            skip += self.batch_limit

        return collected