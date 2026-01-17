from datetime import datetime
import uuid
from pydantic import BaseModel


class DealObj(BaseModel):
    transaction_number: str
    instance_date: datetime
    payload: str
    load_ts: datetime
    ingestion_id: uuid.UUID