from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from utils.helper import *
from airflow.models import Variable
from utils.snowflake_setup import snowflake_connection

default_args = {
    'owner': Variable.get("USER"),
    'retries': 3,  # How many times to retry on failure?
    'retry_delay': timedelta(minutes=5),
}

with DAG(
      dag_id='stock-data-pipeline',  # What should you name it?
      default_args=default_args,
      schedule='30 19 * * *',  # Remember: daily at 6am?
      start_date=datetime(2026, 1, 27),
      catchup=False,
  ) as dag:
        task1 = PythonOperator(
            task_id="run_fetch_and_staging",
            python_callable=fetch_and_staging,
            op_kwargs={"ticker":"AAPL", "period":"2d", "interval":"1h"}
        )

        task2 = PythonOperator(
            task_id="run_merge",
            python_callable=merge_to_main
        )

        task1 >> task2