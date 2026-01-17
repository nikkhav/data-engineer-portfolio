from __future__ import annotations

from airflow.sdk import dag, task
from datetime import datetime, timezone

from tasks.s3_tasks import list_new_s3_objects
from tasks.vectorisation_tasks import vectorise_documents
from tasks.service_db_tasks import save_service_records


@dag(
    dag_id="document_vectorisation",
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["RAG", "vector", "S3"],
)
def document_vectorisation_dag():

    @task(task_id="get_new_files")
    def get_files():
        return list_new_s3_objects()

    @task(task_id="vectorise_files")
    def vectorise(files: list[dict]):
        return vectorise_documents(files)

    @task(task_id="save_service_records")
    def save(files: list[dict]):
        save_service_records(files)

    files = get_files()
    vectorised = vectorise(files)
    save(vectorised)


document_vectorisation_dag()
