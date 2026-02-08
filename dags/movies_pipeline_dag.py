"""
Movies Data Pipeline DAG
========================
Orchestrates the complete data pipeline:
1. Upload CSV to S3
2. Load data from S3 to Snowflake RAW layer
3. Run dbt staging models
4. Run dbt marts models (Star Schema)
5. Run dbt tests

Schedule: Daily at 6 AM
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import os

# ====================================
# Default Arguments
# ====================================
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ====================================
# DAG Definition
# ====================================
with DAG(
    dag_id='movies_pipeline',
    default_args=default_args,
    description='Complete Movies Data Pipeline: CSV → S3 → Snowflake → dbt',
    schedule='0 6 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['movies', 'etl', 'snowflake', 'dbt'],
) as dag:

    # ====================================
    # Task 1: Upload CSV to S3
    # ====================================
    def upload_csv_to_s3_task():
        """Upload local CSV file to S3 bucket"""
        import sys
        sys.path.insert(0, '/opt/airflow/scripts')
        from upload_to_s3 import upload_to_s3
        
        print("📤 Starting upload to S3...")
        result = upload_to_s3(add_timestamp=False)
        print(f"✅ Upload completed: {result}")
        return result

    upload_to_s3 = PythonOperator(
        task_id='upload_csv_to_s3',
        python_callable=upload_csv_to_s3_task,
    )

    # ====================================
    # Task 2: Create Snowflake RAW Table
    # ====================================
    create_raw_table = SnowflakeOperator(
        task_id='create_raw_table',
        snowflake_conn_id='snowflake_default',
        sql="""
        CREATE TABLE IF NOT EXISTS movies_db.raw.movies_raw (
            rank NUMBER,
            title VARCHAR(500),
            year NUMBER,
            genres VARCHAR(200),
            director VARCHAR(200),
            main_actors VARCHAR(500),
            country VARCHAR(200),
            imdb_rating FLOAT,
            rotten_tomatoes_pct NUMBER,
            runtime_mins NUMBER,
            language VARCHAR(200),
            oscars_won NUMBER,
            box_office_million FLOAT,
            metacritic_score NUMBER,
            loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        );
        """,
    )

    # ====================================
    # Task 3: Load S3 to Snowflake RAW
    # ====================================
    load_s3_to_snowflake = SnowflakeOperator(
        task_id='load_s3_to_snowflake',
        snowflake_conn_id='snowflake_default',
        sql=f"""
        -- Truncate existing data
        TRUNCATE TABLE movies_db.raw.movies_raw;

        -- Copy from S3
        COPY INTO movies_db.raw.movies_raw (
            rank, title, year, genres, director, main_actors, 
            country, imdb_rating, rotten_tomatoes_pct, runtime_mins,
            language, oscars_won, box_office_million, metacritic_score
        )
        FROM 's3://{os.getenv("S3_BUCKET_NAME")}/raw/top_100_movies_full_best_effort.csv'
        CREDENTIALS = (
            AWS_KEY_ID = '{os.getenv("AWS_ACCESS_KEY_ID")}'
            AWS_SECRET_KEY = '{os.getenv("AWS_SECRET_ACCESS_KEY")}'
        )
        FILE_FORMAT = (
            TYPE = 'CSV'
            FIELD_DELIMITER = ','
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            NULL_IF = ('', 'NULL', 'null')
            EMPTY_FIELD_AS_NULL = TRUE
            TRIM_SPACE = TRUE
        )
        ON_ERROR = 'CONTINUE';

        -- Verify count
        SELECT COUNT(*) as total_records FROM movies_db.raw.movies_raw;
        """,
    )

    # ====================================
    # Task 4: dbt deps
    # ====================================
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command='cd /opt/airflow/movies_dbt && dbt deps --profiles-dir .',
    )

    # ====================================
    # Task 5: dbt Run Staging Models
    # ====================================
    dbt_run_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command='cd /opt/airflow/movies_dbt && dbt run --select staging --target dev --profiles-dir .',
    )

    # ====================================
    # Task 6: dbt Run Marts Models
    # ====================================
    dbt_run_marts = BashOperator(
        task_id='dbt_run_marts',
        bash_command='cd /opt/airflow/movies_dbt && dbt run --select marts --target prod --profiles-dir .',
    )

    # ====================================
    # Task 7: dbt Test All Models
    # ====================================
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/movies_dbt && dbt test --profiles-dir .',
    )

    # ====================================
    # Task 8: Generate dbt Docs
    # ====================================
    dbt_docs_generate = BashOperator(
        task_id='dbt_docs_generate',
        bash_command='cd /opt/airflow/movies_dbt && dbt docs generate --profiles-dir .',
    )

    # ====================================
    # Define Task Dependencies
    # ====================================
    upload_to_s3 >> create_raw_table >> load_s3_to_snowflake >> dbt_deps
    dbt_deps >> dbt_run_staging >> dbt_run_marts >> dbt_test >> dbt_docs_generate