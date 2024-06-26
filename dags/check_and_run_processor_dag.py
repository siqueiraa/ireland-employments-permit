import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from custom_sensors import ContentChangeSensor

# Define the URL and previous date for comparison
URL = "https://enterprise.gov.ie/en/publications/employment-permit-statistics-{}.html".format(datetime.now().year)
PREVIOUS_DATE = "4th June 2023"  # Replace with the actual previous date

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'check_and_run_processor',
    default_args=default_args,
    description='Check if content has changed and run processor.py',
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# Define the sensor task
check_content_change = ContentChangeSensor(
    task_id='check_content_change',
    url=URL,
    previous_date=PREVIOUS_DATE,
    poke_interval=600,  # Check every 10 minutes
    timeout=3600,  # Timeout after 1 hour
    dag=dag,
)

# Define the Python task to run processor.py
def run_processor():
    venv_python = '/app/.venv/bin/python'
    os.system(f'{venv_python} /app/processor.py')

run_processor_task = PythonOperator(
    task_id='run_processor',
    python_callable=run_processor,
    dag=dag,
)

# Set the task dependencies
check_content_change >> run_processor_task
