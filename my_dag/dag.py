import datetime
import random
from airflow import DAG
from airflow.utils.state import State
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.datetime(2021, 1, 19),
    "email": ["chuan.wang@dragonplus.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": datetime.timedelta(seconds=5),
}


def on_dag_failure(context):
    message = ""
    dag_run = context.get("dag_run")
    for task_instance in dag_run.get_task_instances(state=State.FAILED):
        message += str(task_instance) + "\n"
    print(message)


def dummyRun():
    val = 0
    for i in (random.randint(1, 100) for _ in range(100)):
        print(i)
        val += i
    print(val)


dag = DAG(
    "decoration",
    default_args=default_args,
    schedule_interval="0 8 * * *",
    max_active_runs=1,
    on_failure_callback=on_dag_failure,
)


finished = BashOperator(
    task_id="finished",
    bash_command="echo finished",
    dag=dag,
)


pythoned = PythonOperator(
    task_id="pythoned",
    dag=dag,
    python_callable=dummyRun
)


pythoned >> finished