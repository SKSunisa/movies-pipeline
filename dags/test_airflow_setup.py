"""
Simple Test DAG
===============
ทดสอบว่า Airflow ทำงานได้ปกติ
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    'owner': 'test',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def print_hello():
    print("Hello from Airflow!")
    print("Python operator is working!")
    return "success"

def check_environment():
    import os
    print("🔍 Checking environment variables:")
    env_vars = [
        'AWS_ACCESS_KEY_ID',
        'S3_BUCKET_NAME',
        'SNOWFLAKE_ACCOUNT',
        'SNOWFLAKE_USER',
        'SNOWFLAKE_DATABASE',
    ]
    for var in env_vars:
        value = os.getenv(var, 'NOT_SET')
        masked = value[:4] + '***' if value != 'NOT_SET' and len(value) > 4 else value
        print(f"  {var}: {masked}")

with DAG(
    dag_id='test_airflow_setup',
    default_args=default_args,
    description='Test DAG to verify Airflow setup',
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['test'],
) as dag:

    hello_task = PythonOperator(
        task_id='say_hello',
        python_callable=print_hello,
    )

    check_env = PythonOperator(
        task_id='check_environment',
        python_callable=check_environment,
    )

    bash_test = BashOperator(
        task_id='bash_test',
        bash_command='echo "✅ Bash operator is working!" && date',
    )

    hello_task >> check_env >> bash_test