from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.models import Variable

default_args = {"owner": "data-eng", "retries": 0}

with DAG(
    dag_id="run_databricks_job_daily",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 2 * * *",   # 02:00 daily
    catchup=False,
    default_args=default_args,
    tags=["databricks","prod"]
) as dag:

    # Put your job id in an Airflow Variable (EXPORT_ORDERS_JOB_ID pattern reused)
    run_job = DatabricksRunNowOperator(
        task_id="run_job",
        databricks_conn_id="databricks_default",
        job_id=int(Variable.get("DATABRICKS_JOB_ID")),   # set this in Airflow UI
        notebook_params={
            "AS_OF_DATE": "{{ ds }}"
        }
    )
