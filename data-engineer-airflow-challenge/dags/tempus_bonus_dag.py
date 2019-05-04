"""DAG to retrieve top headlines by keywords."""

import os
from datetime import (datetime, timedelta)
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from challenge.active import Headlines


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 5, 2),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# News API
NEWS = Headlines(os.environ['API_KEY'])


# DAG Object
dag = DAG(
    'tempus_bonus_dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1),  # DAG will run once daily
    catchup=False,
)


get_keyword_headlines_task = PythonOperator(
    task_id='get_keyword_headlines',
    python_callable=NEWS.get_keyword_headlines,
    params={'keywords': ['Tempus Labs',
                         'Eric Lefkofsky', 'Cancer', 'Immunotherapy']},
    provide_context=True,
    dag=dag
)


create_csv_task = PythonOperator(
    task_id='create_csv_keyword',
    python_callable=NEWS.create_csvs,
    dag=dag
)


upload_s3_task = PythonOperator(
    task_id='upload_s3_keyword',
    python_callable=NEWS.upload_s3,
    dag=dag
)


# A visual representation of the following should be viewable at:
# http://localhost:8080/admin/airflow/graph?dag_id=sample_dag
get_keyword_headlines_task.set_downstream(create_csv_task)
create_csv_task.set_downstream(upload_s3_task)
