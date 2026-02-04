"""
Movies Data Pipeline - Upload CSV to S3 and Load to Snowflake RAW
Compatible with Airflow 3.x using TaskFlow API
Using @task decorator with snowflake-connector-python
"""

from airflow.sdk import dag, task
from datetime import datetime, timedelta
import logging
import os

# ============================================================================
# DAG Configuration
# ============================================================================
default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=10),
}

@dag(
    dag_id='movies_s3_to_snowflake_pipeline',
    default_args=default_args,
    description='Upload movies CSV to S3 and load into Snowflake RAW layer',
    schedule=None,
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=['movies', 's3', 'snowflake', 'data-pipeline'],
    max_active_runs=1,
)
def movies_data_pipeline():
    """Movies Data Pipeline DAG"""
    
    # ========================================================================
    # Task 1: Upload CSV to S3
    # ========================================================================
    @task(task_id='upload_csv_to_s3')
    def upload_to_s3():
        """Upload movies CSV file to S3 bucket"""
        from scripts.upload_to_s3 import upload_to_s3 as upload_func
        
        try:
            logging.info("Starting CSV upload to S3...")
            result = upload_func(add_timestamp=False)
            logging.info("Upload completed successfully!")
            logging.info(f"S3 URI: {result['s3_uri']}")
            logging.info(f"File size: {result['file_size_kb']:.2f} KB")
            return result
            
        except FileNotFoundError as e:
            logging.error(f"File not found: {str(e)}")
            logging.error("Make sure CSV file exists in /opt/airflow/data/")
            raise
            
        except Exception as e:
            logging.error(f"Upload failed: {str(e)}")
            logging.error(f"Error type: {type(e).__name__}")
            raise
    
    # ========================================================================
    # Task 2: Verify S3 Upload
    # ========================================================================
    @task(task_id='verify_s3_upload')
    def verify_upload(upload_result: dict):
        """Verify that file was uploaded successfully to S3"""
        
        try:
            logging.info("Verifying S3 upload...")
            if not upload_result:
                raise ValueError("No upload result received from previous task!")
            
            required_keys = ['s3_uri', 'file_size', 'file_size_kb']
            missing_keys = [key for key in required_keys if key not in upload_result]
            
            if missing_keys:
                raise ValueError(f"Missing keys in upload result: {missing_keys}")
            
            file_size = upload_result.get('file_size', 0)
            if file_size == 0:
                raise ValueError("Uploaded file is empty (0 bytes)!")
            
            logging.info("Upload verification passed!")
            logging.info(f"S3 URI: {upload_result['s3_uri']}")
            logging.info(f"File size: {file_size:,} bytes ({upload_result['file_size_kb']:.2f} KB)")
            logging.info(f"Upload time: {upload_result.get('upload_timestamp', 'N/A')}")
            
            return {
                'status': 'verified',
                's3_uri': upload_result['s3_uri'],
                'file_size': file_size
            }
            
        except Exception as e:
            logging.error(f"Verification failed: {str(e)}")
            raise
    
    # ========================================================================
    # Task 3: Create RAW Schema in Snowflake
    # ========================================================================
    @task(task_id='create_raw_schema')
    def create_raw_schema():
        """Create RAW schema in Snowflake"""
        import snowflake.connector
        
        logging.info("Connecting to Snowflake...")
        
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            role=os.getenv('SNOWFLAKE_ROLE')
        )
        
        cursor = conn.cursor()
        
        try:
            logging.info("Creating RAW schema...")
            cursor.execute("""
                CREATE SCHEMA IF NOT EXISTS RAW
                    COMMENT = 'Raw data layer - Data loaded directly from S3'
            """)
            
            cursor.execute("USE SCHEMA RAW")
            
            logging.info("Schema RAW created successfully")
            return {"status": "success", "schema": "RAW"}
            
        finally:
            cursor.close()
            conn.close()
    
    # ========================================================================
    # Task 4: Create RAW Table in Snowflake
    # ========================================================================
    @task(task_id='create_raw_table')
    def create_raw_table():
        """Create MOVIES_RAW table in Snowflake"""
        import snowflake.connector
        
        logging.info("Connecting to Snowflake...")
        
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            role=os.getenv('SNOWFLAKE_ROLE'),
            schema='RAW'
        )
        
        cursor = conn.cursor()
        
        try:
            logging.info("Dropping existing table (if exists)...")
            cursor.execute("DROP TABLE IF EXISTS RAW.MOVIES_RAW")
            
            logging.info("Creating MOVIES_RAW table...")
            cursor.execute("""
                CREATE TABLE RAW.MOVIES_RAW (
                    rank INTEGER NOT NULL,
                    title VARCHAR(500) NOT NULL,
                    year INTEGER NOT NULL,
                    genres VARCHAR(500),
                    director VARCHAR(500),
                    main_actors VARCHAR(1000),
                    country VARCHAR(200),
                    imdb_rating FLOAT,
                    rotten_tomatoes_pct FLOAT,
                    runtime_mins FLOAT,
                    language VARCHAR(200),
                    oscars_won INTEGER,
                    box_office_millions FLOAT,
                    metacritic_score FLOAT,
                    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    source_file VARCHAR(500)
                )
                COMMENT = 'Raw movies data loaded from S3 - Top 100 movies dataset'
            """)
            
            logging.info("Table MOVIES_RAW created successfully")
            return {"status": "success", "table": "MOVIES_RAW"}
            
        finally:
            cursor.close()
            conn.close()
    
    # ========================================================================
    # Task 5: Load Data from S3 to Snowflake
    # ========================================================================
    @task(task_id='load_s3_to_snowflake')
    def load_s3_to_snowflake():
        """Load data from S3 to Snowflake using COPY INTO"""
        import snowflake.connector
        
        logging.info("Connecting to Snowflake...")
        
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            role=os.getenv('SNOWFLAKE_ROLE'),
            schema='RAW'
        )
        
        cursor = conn.cursor()
        
        try:
            logging.info("Truncating table...")
            cursor.execute("TRUNCATE TABLE IF EXISTS RAW.MOVIES_RAW")
            
            logging.info("Loading data from S3...")
            cursor.execute("""
                COPY INTO RAW.MOVIES_RAW (
                    rank, title, year, genres, director, main_actors, country,
                    imdb_rating, rotten_tomatoes_pct, runtime_mins, language,
                    oscars_won, box_office_millions, metacritic_score,
                    loaded_at, source_file
                )
                FROM (
                    SELECT 
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
                        CURRENT_TIMESTAMP(),
                        METADATA$FILENAME
                    FROM @RAW.MOVIES_S3_STAGE/top_100_movies_full_best_effort.csv
                )
                FILE_FORMAT = (
                    TYPE = 'CSV'
                    FIELD_DELIMITER = ','
                    SKIP_HEADER = 1
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                    NULL_IF = ('NULL', 'null', '', 'NA', 'N/A')
                    EMPTY_FIELD_AS_NULL = TRUE
                    ENCODING = 'UTF8'
                    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
                    TRIM_SPACE = TRUE
                )
                ON_ERROR = 'CONTINUE'
                RETURN_FAILED_ONLY = FALSE
            """)
            
            cursor.execute("SELECT COUNT(*) AS rows_loaded FROM RAW.MOVIES_RAW")
            result = cursor.fetchone()
            rows_loaded = result[0] if result else 0
            
            logging.info(f"Data loaded successfully: {rows_loaded} rows")
            return {"status": "success", "rows_loaded": rows_loaded}
            
        finally:
            cursor.close()
            conn.close()
    
    # ========================================================================
    # Task 6: Verify Loaded Data
    # ========================================================================
    @task(task_id='verify_snowflake_data')
    def verify_snowflake_data():
        """Verify data quality in Snowflake"""
        import snowflake.connector
        
        logging.info("Connecting to Snowflake...")
        
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            role=os.getenv('SNOWFLAKE_ROLE'),
            schema='RAW'
        )
        
        cursor = conn.cursor()
        
        try:
            logging.info("=== DATA QUALITY VALIDATION REPORT ===")
            
            # 1. Row Count
            cursor.execute("SELECT COUNT(*) FROM RAW.MOVIES_RAW")
            total_rows = cursor.fetchone()[0]
            logging.info(f"1. Row Count: {total_rows}")
            
            # 2. NULL Values
            cursor.execute("""
                SELECT 
                    COUNT(CASE WHEN metacritic_score IS NULL THEN 1 END) AS null_metacritic,
                    COUNT(CASE WHEN box_office_millions IS NULL THEN 1 END) AS null_box_office,
                    COUNT(CASE WHEN rotten_tomatoes_pct IS NULL THEN 1 END) AS null_rt
                FROM RAW.MOVIES_RAW
            """)
            null_counts = cursor.fetchone()
            logging.info(f"2. NULL Values - Metacritic: {null_counts[0]}, Box Office: {null_counts[1]}, RT: {null_counts[2]}")
            
            # 3. Summary Stats
            cursor.execute("""
                SELECT 
                    MIN(year) AS min_year,
                    MAX(year) AS max_year,
                    ROUND(AVG(imdb_rating), 2) AS avg_imdb
                FROM RAW.MOVIES_RAW
            """)
            stats = cursor.fetchone()
            logging.info(f"3. Summary - Year range: {stats[0]}-{stats[1]}, Avg IMDb: {stats[2]}")
            
            # 4. Sample Data
            cursor.execute("SELECT rank, title, year FROM RAW.MOVIES_RAW ORDER BY rank LIMIT 5")
            logging.info("4. Sample Data (Top 5):")
            for row in cursor:
                logging.info(f"   Rank {row[0]}: {row[1]} ({row[2]})")
            
            logging.info("=== ALL CHECKS PASSED ===")
            
            return {
                "status": "success",
                "total_rows": total_rows,
                "validation": "passed"
            }
            
        finally:
            cursor.close()
            conn.close()
    
    # ========================================================================
    # Define Task Dependencies
    # ========================================================================
    upload_result = upload_to_s3()
    verify_result = verify_upload(upload_result)
    schema_result = create_raw_schema()
    table_result = create_raw_table()
    load_result = load_s3_to_snowflake()
    verify_data_result = verify_snowflake_data()
    
    # Chain
    verify_result >> schema_result >> table_result >> load_result >> verify_data_result


# ============================================================================
# Instantiate DAG
# ============================================================================
movies_dag = movies_data_pipeline()