import subprocess
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'quoch',
    'start_date': datetime(2024, 5, 10),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'email': ['20067161.quoc@student.iuh.edu.vn'],
    'retry_delay': timedelta(minutes=5),
}

def crawl_func():
    command = ["scrapy", "runspider", "/opt/airflow/crawl/crawldata/crawldata/spiders/hoatuoi.py"]
    subprocess.run(command)

def clean_func():
    command = ["python3", "/opt/airflow/dags/clean_data.py"]
    subprocess.run(command)

def send_mail_func():
    command = ["python3", "/opt/airflow/dags/send_email.py"]
    subprocess.run(command)

with DAG(
    'dag_result1',
    default_args=default_args,
    description='A simple DAG to crawl data from fpt career, clean data, and send email',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:
    crawl_task = PythonOperator(
        task_id='crawl_task',
        python_callable=crawl_func,
        dag=dag
    )

    clean_task = PythonOperator(
        task_id='clean_task',
        python_callable=clean_func,
        dag=dag
    )

    send_mail_task = PythonOperator(
        task_id='send_mail_task',
        python_callable=send_mail_func,
        dag=dag
    )

crawl_task >> clean_task >> send_mail_task