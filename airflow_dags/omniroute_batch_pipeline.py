from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

#########################################
# DEFAULT ARGS
#########################################

default_args = {
    "owner": "ubuntu",
    "start_date": datetime(2026, 1, 1),
    "retries": 1
}

#########################################
# DAG
#########################################

dag = DAG(
    dag_id="omniroute_batch_pipeline",
    default_args=default_args,
    schedule="0 5 * * *",
    catchup=False
)

#########################################
# TASKS
#########################################

vehicle_registry_task = BashOperator(
    task_id="vehicle_registry_silver",
    bash_command="""
    source /home/ubuntu/omniroute-project/venvs/spark_venv/bin/activate &&
    cd /home/ubuntu/omniroute-project/spark_jobs/batch &&
    spark-submit vehicle_registry_etl.py
    """,
    dag=dag
)

vehicle_assignment_task = BashOperator(
    task_id="vehicle_assignment_silver",
    bash_command="""
    source /home/ubuntu/omniroute-project/venvs/spark_venv/bin/activate &&
    cd /home/ubuntu/omniroute-project/spark_jobs/batch &&
    spark-submit vehicle_assignment_silver_etl.py
    """,
    dag=dag
)

asset_history_task = BashOperator(
    task_id="asset_history_scd2_gold",
    bash_command="""
    source /home/ubuntu/omniroute-project/venvs/spark_venv/bin/activate &&
    cd /home/ubuntu/omniroute-project/spark_jobs/batch &&
    spark-submit vehicle_assignment_scd2_gold.py
    """,
    dag=dag
)

fuel_efficiency_task = BashOperator(
    task_id="fuel_efficiency_gold",
    bash_command="""
    source /home/ubuntu/omniroute-project/venvs/spark_venv/bin/activate &&
    cd /home/ubuntu/omniroute-project/spark_jobs/batch &&
    spark-submit fuel_efficiency_gold.py
    """,
    dag=dag
)

#########################################
# PIPELINE ORDER
#########################################

vehicle_registry_task >> vehicle_assignment_task

vehicle_assignment_task >> asset_history_task

vehicle_registry_task >> fuel_efficiency_task
