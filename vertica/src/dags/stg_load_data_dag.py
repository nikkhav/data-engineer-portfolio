import pandas as pd
from vertica_python import connect
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago


def load_from_file(table: str, key: str):
    filename = f"/data/{key}"
    df = pd.read_csv(filename)

    df["user_id_from"] = pd.array(df["user_id_from"], dtype="Int64")
    df.to_csv(filename, index=False)

    creds = Variable.get("vertica_creds", deserialize_json=True)
    conn_info = {
        "host": creds["host"],
        "port": int(creds.get("port", 5433)),
        "user": creds["user"],
        "password": creds["password"],
        "database": creds["database"],
        "autocommit": False,
    }

    with connect(**conn_info) as conn:
        cur = conn.cursor()

        cur.execute(f"TRUNCATE TABLE {table};")
        with open(filename, "r") as f:
            cur.copy(
                f"""
                COPY {table}
                FROM STDIN
                DELIMITER ','
                ENCLOSED BY '\"'
                NULL AS ''
                DIRECT;
                """,
                f.read(),
            )
        cur.execute("SELECT GET_NUM_ACCEPTED_ROWS();")
        rows_inserted = cur.fetchone()[0]

        conn.commit()
        cur.close()

    print(f"Streamed cleaned {filename} successfully into {table}")
    print(f"Rows inserted: {rows_inserted}")
    return rows_inserted


@dag(
    dag_id="stg_load_data_dag",
    description="Load CSV data into Vertica staging tables",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["staging", "vertica"],
)
def stg_load_data_dag():
    @task(task_id="load_group_log")
    def load_group_log_task():
        load_from_file(
            table="STV202510067__STAGING.group_log",
            key="group_log.csv",
        )
        return "Data loaded successfully into staging.group_log"

    load_group_log_task()


dag = stg_load_data_dag()
