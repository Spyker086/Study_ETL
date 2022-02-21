from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

def dump_data(**kwargs):
    phook = PostgresHook(postgres_conn_id="postgres_target")
    conn = phook.get_conn()
    for tabname in ['customer',
                    'lineitem',
                    'nation',
                    'orders',
                    'part',
                    'partsupp',
                    'region',
                    'supplier']:
        with conn.cursor() as cur:
            with open("dump/{}.csv".format(tabname), "w") as f:
                cur.copy_expert("COPY {} TO STDOUT WITH DELIMITER ',' CSV HEADER;".format(tabname), f)

def load_data(**kwargs):
    phook = PostgresHook(postgres_conn_id="postgres_target2")
    conn = phook.get_conn()
    conn.autocommit = True
    for tabname in ['customer',
                    'lineitem',
                    'nation',
                    'orders',
                    'part',
                    'partsupp',
                    'region',
                    'supplier']:
        with conn.cursor() as cur:
            with open("dump/{}.csv".format(tabname), "r") as f:
                cur.copy_expert("COPY {} FROM STDIN WITH DELIMITER ',' CSV HEADER;".format(tabname), f)


DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2021, 6, 3),
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
}

with DAG(
    dag_id="dump_data",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    tags=['data-flow'],
    catchup=False
) as dag:

    dump_data = PythonOperator(
    task_id = 'dump_my_data',
    python_callable = dump_data
    )

    load_data = PythonOperator(
    task_id = 'load_my_data',
    python_callable = load_data
    )

    dump_data >> load_data