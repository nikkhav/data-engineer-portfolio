import pandas as pd
from io import StringIO
from pydantic import BaseModel
from datetime import datetime

class CurrencyObj(BaseModel):
    currency_code: int
    currency_code_with: int
    currency_with_div: float
    date_update: datetime

class TransactionObj(BaseModel):
    operation_id: str
    account_number_from: int
    account_number_to: int
    currency_code: int
    country: str
    status: str
    transaction_type: str
    amount: int
    transaction_dt: datetime


class S3OriginRepository:
    def __init__(self, s3_client, bucket_name, logger):
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.log = logger
        self.file_name_template = "transactions_batch_{batch_number}.csv"

    def list_files(self):
        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.bucket_name)
        files = []
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    files.append(obj['Key'])
        return files

    def read_file(self, key):
        response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
        return response['Body'].read().decode('utf-8')

    def list_currencies(
            self,
            pg_conn,
            wf_repo,
            workflow_key: str
    ) -> tuple[pd.DataFrame, datetime]:
        wf_setting = wf_repo.get_setting(pg_conn, workflow_key)

        if wf_setting is None:
            last_loaded_date = datetime.min
        else:
            last_date_str = wf_setting.workflow_settings.get("last_loaded_date")
            last_loaded_date = (
                datetime.fromisoformat(last_date_str)
                if last_date_str else datetime.min
            )

        self.log.info(f"Last loaded date: {last_loaded_date}")

        files = self.list_files()
        if "currencies_history.csv" not in files:
            self.log.warning("currencies_history.csv not found in S3 bucket")
            return pd.DataFrame(), last_loaded_date

        file_content = self.read_file("currencies_history.csv")
        df = pd.read_csv(StringIO(file_content))

        df["date_update"] = pd.to_datetime(df["date_update"])

        df_new = df[df["date_update"] > last_loaded_date]

        if df_new.empty:
            self.log.info("No new currency rows to load.")
            return pd.DataFrame(), last_loaded_date

        rows = [CurrencyObj(**row).dict() for row in df_new.to_dict("records")]

        df_validated = pd.DataFrame(rows)

        new_last_date = df_validated["date_update"].max()

        return df_validated, new_last_date

    def _filter_transaction_files(self, files):
        batch_files = []
        for f in files:
            if f.startswith("transactions_batch_") and f.endswith(".csv"):
                try:
                    num = int(f.split("_")[-1].split(".")[0])
                    batch_files.append((num, f))
                except ValueError:
                    continue
        return [f for _, f in sorted(batch_files, key=lambda x: x[0])]

    def list_transactions(
            self,
            pg_conn,
            wf_repo,
            workflow_key: str
    ) -> tuple[pd.DataFrame, int]:

        wf_setting = wf_repo.get_setting(pg_conn, workflow_key)

        if wf_setting is None:
            last_loaded_batch = 0
        else:
            last_loaded_batch = wf_setting.workflow_settings.get("last_loaded_batch", 0)

        next_batch = last_loaded_batch + 1

        self.log.info(f"Last loaded batch: {last_loaded_batch}, next batch: {next_batch}")

        files = self._filter_transaction_files(self.list_files())
        available_batches = [int(f.split("_")[-1].split(".")[0]) for f in files]

        transactions = []
        loaded_batches = []

        for batch_num, file_name in zip(available_batches, files):

            if batch_num < next_batch:
                continue

            loaded_batches.append(batch_num)

            self.log.info(f"Loading batch {batch_num}: {file_name}")

            file_content = self.read_file(file_name)
            df = pd.read_csv(StringIO(file_content))
            df["transaction_dt"] = pd.to_datetime(df["transaction_dt"])

            for row in df.to_dict("records"):
                transactions.append(TransactionObj(**row))

        df_all = pd.DataFrame([t.dict() for t in transactions])

        if loaded_batches:
            new_last_batch = max(loaded_batches)
        else:
            new_last_batch = last_loaded_batch

        return df_all, new_last_batch
