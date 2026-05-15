from airflow import DAG

from airflow.operators.bash import BashOperator

from datetime import datetime

#########################################
# DEFAULT ARGS
#########################################

default_args = {
    "owner": "ubuntu",
    "depends_on_past": False,
    "start_date": datetime(2026, 5, 15),
    "retries": 1
}

#########################################
# CREATE DAG
#########################################

dag = DAG(
    dag_id="monthly_driver_cooldown",
    default_args=default_args,
    schedule=None,
#"0 5 1 * *",
    catchup=False,
    description="Monthly reset of driver strikes and rate restoration"
)

#########################################
# MONTHLY COOLDOWN TASK
#########################################

cooldown_task = BashOperator(
    task_id="reset_driver_safety",
    bash_command="""
    source /home/ubuntu/omniroute-project/venvs/spark_venv/bin/activate &&
    cd /home/ubuntu/omniroute-project/spark_jobs/batch &&
    spark-submit monthly_driver_cooldown.py
    """,
    dag=dag
)
