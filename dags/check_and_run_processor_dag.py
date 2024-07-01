import os
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from custom_sensors import ContentChangeSensor

# Define the URL with the current year dynamically
current_year = datetime.now().year
URL = f"https://enterprise.gov.ie/en/publications/employment-permit-statistics-{current_year}.html"

# Define the path to the date file
DATE_FILE_PATH = "date_file.txt"

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    'check_and_run_processor_01',
    default_args=default_args,
    description='Check if content has changed and run processor.py',
    schedule=timedelta(minutes=60),
    start_date=datetime(2024, 7, 1),
    catchup=False,
    is_paused_upon_creation=False,
)

# Define the sensor task
check_content_change = ContentChangeSensor(
    task_id='check_content_change',
    url=URL,
    date_file_path=DATE_FILE_PATH,
    poke_interval=60,
    timeout=20,
    mode='poke',  # Ensure the mode is set to 'poke' if it needs to stop immediately
    dag=dag,
)

# Define a Python function to run processor.py
def run_processor(ti):
    #root = '/home/rafael/PycharmProjects/ireland-employments-permit'
    # venv_python = f'{root}/.venv/bin/python'
    root = '/app'
    venv_python = f'python'
    logging.info(f"Root Path {root}")
    logging.info(f"Venv {venv_python}")

    content_changed = ti.xcom_pull(task_ids='check_content_change', key='content_changed')
    if not content_changed:
        logging.info("Content has not changed. Skipping processor execution.")
        return

    try:
        logging.info(f'Running {venv_python} {root}/processor.py')
        result = os.system(f'{venv_python} {root}/processor.py')
        if result != 0:
            raise Exception(f'processor.py failed with exit code {result}')
        logging.info(f'Running {venv_python} {root}/update_google_sheet.py')
        result = os.system(f'{venv_python} {root}/update_google_sheet.py')
        if result != 0:
            raise Exception(f'update_google_sheet.py failed with exit code {result}')
        logging.info('Both scripts executed successfully.')
    except Exception as e:
        logging.error(f'Error during script execution: {e}')
        raise

# Define the Python task
run_processor_task = PythonOperator(
    task_id='run_processor',
    python_callable=run_processor,
    dag=dag,
)

# Define task dependencies
check_content_change >> run_processor_task

# Additional logging to confirm task dependency setup
dag.log.info("Task dependencies set: check_content_change >> run_processor_task")
