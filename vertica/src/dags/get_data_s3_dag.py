import os
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def get_s3_file(bucket: str, key: str, aws_conn_id: str = "yandex_s3_connection"):
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    local_dir = "/data"
    local_file = f"{local_dir}/{key}"

    os.makedirs(local_dir, exist_ok=True)

    tmp_path = s3_hook.download_file(
        key=key,
        bucket_name=bucket,
        local_path="/data",
    )
    final_path = os.path.join("/data", key)
    os.replace(tmp_path, final_path)

    print(f"File {key} downloaded from bucket {bucket} to {local_file}")
    return local_file


def log_file_contents(file_path: str, max_lines: int = 10):
    try:
        with open(file_path, "r") as file:
            print(f"📄 Showing first {max_lines} lines of {file_path}:\n")
            for i, line in enumerate(file):
                if i >= max_lines:
                    break
                print(line.rstrip())
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")


@dag(
    dag_id="get_data_s3_dag",
    description="DAG to get data from S3",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["aws", "s3", "csv"],
)
def get_data_s3_dag():
    @task(task_id="get_group_log")
    def get_group_log_task() -> str:
        return get_s3_file(bucket="sprint6", key="group_log.csv", aws_conn_id="yandex_s3_connection")

    @task(task_id="log_group_log")
    def log_group_log_task(file_path: str):
        log_file_contents(file_path)
        return "File logged successfully"

    file_path = get_group_log_task()
    log_group_log_task(file_path)


dag = get_data_s3_dag()
