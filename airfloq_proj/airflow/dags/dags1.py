from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

tables = ['customer', 'lineitem', 'lineitem', 'nation', 'orders', 'partsupp', 'part', 'region', 'supplier']

def dump_data(**kwargs):
    phook = PostgresHook(postgres_conn_id="postgres_source")
    conn = phook.get_conn()
    with conn.cursor() as cur:
        for table in tables:
            q = f"COPY {table} TO STDOUT WITH DELIMITER ',' CSV HEADER;"
            with open(f'file{table}.csv', 'w') as f:
                cur.copy_expert(q, f)


def copy_data(**kwargs):
    phook = PostgresHook(postgres_conn_id="postgres_target")
    conn = phook.get_conn()
    with conn.cursor() as cur:
        for table in tables:
            q = f"COPY {table} FROM STDIN WITH DELIMITER ',' CSV HEADER;"
            with open(f'file{table}.csv', 'r') as f:
                cur.copy_expert(q, f)
    conn.commit()


DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2021, 6, 3),
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
}

with DAG(
    dag_id="test_customer",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    tags=['data-flow'],
    catchup=False
) as dag:

    dump_data = PythonOperator(
        task_id = 'dump_my_data',
        python_callable = dump_data
    )
    copy_data = PythonOperator(
        task_id = 'copy_my_data',
        python_callable = copy_data
    )

    dump_data >> copy_data