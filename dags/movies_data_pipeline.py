"""
Movies Data Pipeline - Upload CSV to S3 and Load to Snowflake RAW
Compatible with Airflow 3.x using TaskFlow API
"""

from airflow.sdk import dag, task
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from datetime import datetime, timedelta
import logging


# ============================================================================
# DAG Configuration
# ============================================================================
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

@dag(
    dag_id='movies_s3_to_snowflake_pipeline',
    default_args=default_args,
    description='Upload movies CSV to S3 and load into Snowflake RAW layer',
    schedule=None,  # Manual trigger
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=['movies', 's3', 'snowflake', 'data-pipeline'],
    max_active_runs=1,
)
def movies_data_pipeline():
    """
    Movies Data Pipeline DAG
    
    Flow:
    1. Upload CSV to S3
    2. Verify S3 upload
    3. Create Snowflake RAW schema
    4. Create RAW table
    5. Load data from S3 to Snowflake
    6. Verify loaded data
    """
    
    # ========================================================================
    # Task 1: Upload CSV to S3
    # ========================================================================
    @task(task_id='upload_csv_to_s3')
    def upload_to_s3():
        """Upload movies CSV file to S3 bucket"""
        from scripts.upload_to_s3 import upload_to_s3 as upload_func
        
        try:
            logging.info("Starting CSV upload to S3...")
            
            # Upload file
            result = upload_func(add_timestamp=False)
            
            # Log success
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
            
            # Check if result exists
            if not upload_result:
                raise ValueError("No upload result received from previous task!")
            
            # Validate result structure
            required_keys = ['s3_uri', 'file_size', 'file_size_kb']
            missing_keys = [key for key in required_keys if key not in upload_result]
            
            if missing_keys:
                raise ValueError(f"Missing keys in upload result: {missing_keys}")
            
            # Verify file size
            file_size = upload_result.get('file_size', 0)
            if file_size == 0:
                raise ValueError("Uploaded file is empty (0 bytes)!")
            
            # Log verification success
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
    create_schema = SnowflakeSqlApiOperator(
        task_id='create_raw_schema',
        snowflake_conn_id='snowflake_default',
        sql="""
        -- Create RAW schema if not exists
        CREATE SCHEMA IF NOT EXISTS RAW
            COMMENT = 'Raw data layer - Data loaded directly from S3';
        
        -- Use RAW schema
        USE SCHEMA RAW;
        """,
        dag=None,  # Will be set by decorator
    )
    
    # ========================================================================
    # Task 4: Create RAW Table in Snowflake
    # ========================================================================
    create_table = SnowflakeSqlApiOperator(
        task_id='create_raw_table',
        snowflake_conn_id='snowflake_default',
        sql="""
        -- Drop existing table for clean rebuild
        DROP TABLE IF EXISTS RAW.MOVIES_RAW;
        
        -- Create movies raw table with proper data types
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
        COMMENT = 'Raw movies data loaded from S3 - Top 100 movies dataset';
        
        -- Show table structure
        DESCRIBE TABLE RAW.MOVIES_RAW;
        """,
        dag=None,
    )
    
    # ========================================================================
    # Task 5: Load Data from S3 to Snowflake
    # ========================================================================
    load_data = SnowflakeSqlApiOperator(
        task_id='load_s3_to_snowflake',
        snowflake_conn_id='snowflake_default',
        sql="""
        -- Truncate table first for clean load
        TRUNCATE TABLE IF EXISTS RAW.MOVIES_RAW;
        
        -- Load data from S3 using external stage
        COPY INTO RAW.MOVIES_RAW (
            rank,
            title,
            year,
            genres,
            director,
            main_actors,
            country,
            imdb_rating,
            rotten_tomatoes_pct,
            runtime_mins,
            language,
            oscars_won,
            box_office_millions,
            metacritic_score,
            loaded_at,
            source_file
        )
        FROM (
            SELECT 
                $1,  -- Rank
                $2,  -- Title
                $3,  -- Year
                $4,  -- Genre(s)
                $5,  -- Director
                $6,  -- Main Actor(s)
                $7,  -- Country
                $8,  -- IMDb Rating
                $9,  -- Rotten Tomatoes %
                $10, -- Runtime (mins)
                $11, -- Language
                $12, -- Oscars Won
                $13, -- Box Office ($M)
                $14, -- Metacritic Score
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
        RETURN_FAILED_ONLY = FALSE;
        
        -- Show load summary
        SELECT COUNT(*) AS rows_loaded FROM RAW.MOVIES_RAW;
        """,
        dag=None,
    )
    
    # ========================================================================
    # Task 6: Verify Loaded Data with Comprehensive Quality Checks
    # ========================================================================
    verify_data = SnowflakeSqlApiOperator(
        task_id='verify_snowflake_data',
        snowflake_conn_id='snowflake_default',
        sql="""
        -- ====================================================================
        -- DATA QUALITY VALIDATION CHECKS
        -- ====================================================================
        
        SELECT '=== DATA QUALITY VALIDATION REPORT ===' AS report_section;
        
        -- --------------------------------------------------------------------
        -- 1. ตรวจสอบจำนวนแถวทั้งหมด (Row Count Check)
        -- --------------------------------------------------------------------
        SELECT 
            '1. Row Count Check' AS check_name,
            COUNT(*) AS total_rows
        FROM RAW.MOVIES_RAW;

        
        -- --------------------------------------------------------------------
        -- 2. ตรวจสอบ NULL ในคอลัมน์ที่มี Missing Data (จาก Phase 1)
        -- --------------------------------------------------------------------
        SELECT 
            '2. NULL Values Check' AS check_name,
            COUNT(CASE WHEN metacritic_score IS NULL THEN 1 END) AS null_metacritic_score,
            COUNT(CASE WHEN box_office_millions IS NULL THEN 1 END) AS null_box_office,
            COUNT(CASE WHEN rotten_tomatoes_pct IS NULL THEN 1 END) AS null_rotten_tomatoes,
            COUNT(CASE WHEN imdb_rating IS NULL THEN 1 END) AS null_imdb_rating,
            COUNT(CASE WHEN runtime_mins IS NULL THEN 1 END) AS null_runtime_mins
        FROM RAW.MOVIES_RAW;
        
        -- --------------------------------------------------------------------
        -- 3. ตรวจสอบ Duplicate Records
        -- --------------------------------------------------------------------
        WITH standardized AS (
            SELECT
                rank,
                title,
                year,
                REGEXP_REPLACE(title, '\\s*\\([^)]*\\)', '') AS title_cleaned
            FROM RAW.MOVIES_RAW
        ),
        duplicates_data AS (
            SELECT 
                rank,
                title,
                year,
                title_cleaned,
                COUNT(*) OVER (PARTITION BY title_cleaned) AS occurrence_count
            FROM standardized
        )
        SELECT
            '3. Duplicate Check' AS check_name,
                rank,
                title,
                year,
                title_cleaned
        FROM duplicates_data
        WHERE occurrence_count > 1
        ORDER BY title_cleaned, rank;

        -- --------------------------------------------------------------------
        -- 4. MULTI-VALUE COLUMNS CHECK - Find columns with pipe delimiter '|'
        -- --------------------------------------------------------------------
        
        -- Method 1: Check each column individually
        SELECT '=== Columns with Pipe Delimiter (|) ===' AS section;

        SELECT 'genres' AS column_name,
            CASE WHEN COUNT(CASE WHEN genres LIKE '%|%' THEN 1 END) > 0 
                THEN 'Has pipe delimiter' 
                ELSE 'No pipe delimiter' 
            END AS has_pipe
        FROM RAW.MOVIES_RAW
        UNION ALL
        SELECT 'director',
            CASE WHEN COUNT(CASE WHEN director LIKE '%|%' THEN 1 END) > 0 
                THEN 'Has pipe delimiter' 
                ELSE 'No pipe delimiter' 
            END AS has_pipe
        FROM RAW.MOVIES_RAW
        UNION ALL
        SELECT 'main_actors',
            CASE WHEN COUNT(CASE WHEN main_actors LIKE '%|%' THEN 1 END) > 0 
                THEN 'Has pipe delimiter' 
                ELSE 'No pipe delimiter' 
            END AS has_pipe
        FROM RAW.MOVIES_RAW
        UNION ALL
        SELECT 'country',
            CASE WHEN COUNT(CASE WHEN country LIKE '%|%' THEN 1 END) > 0 
                THEN 'Has pipe delimiter' 
                ELSE 'No pipe delimiter' 
            END AS has_pipe
        FROM RAW.MOVIES_RAW
        UNION ALL
        SELECT 'language',
            CASE WHEN COUNT(CASE WHEN language LIKE '%|%' THEN 1 END) > 0 
                THEN 'Has pipe delimiter' 
                ELSE 'No pipe delimiter' 
            END AS has_pipe
        FROM RAW.MOVIES_RAW;

        -- --------------------------------------------------------------------
        -- 5. ตรวจสอบปี (Year Validation)
        -- ไม่ควรเก่าเกินไป (< 1800) และไม่ควรเป็นอนาคต (> 2025)
        -- --------------------------------------------------------------------
        SELECT 
            '5. Year Validation' AS check_name,
            MIN(year) AS min_year,
            MAX(year) AS max_year,
            COUNT(CASE WHEN year < 1800 OR year > 2025 THEN 1 END) AS invalid_years,
            CASE 
                WHEN COUNT(CASE WHEN year < 1800 OR year > 2025 THEN 1 END) = 0 
                THEN 'PASS' 
                ELSE 'FAIL' 
            END AS status,
            'Expected: 1800 <= year <= 2025' AS expected
        FROM RAW.MOVIES_RAW;
        
        -- แสดงรายการปีที่ผิดปกติ (ถ้ามี)
        SELECT 
            '   Invalid Years Details:' AS detail,
            rank, title, year
        FROM RAW.MOVIES_RAW
        WHERE year < 1800 OR year > 2025
        ORDER BY year;
        
        -- --------------------------------------------------------------------
        -- 6. ตรวจสอบคะแนน IMDb Rating (ต้องอยู่ระหว่าง 0-10)
        -- --------------------------------------------------------------------
        SELECT 
            '6. IMDb Rating Validation' AS check_name,
            ROUND(MIN(imdb_rating), 2) AS min_rating,
            ROUND(MAX(imdb_rating), 2) AS max_rating,
            ROUND(AVG(imdb_rating), 2) AS avg_rating,
            COUNT(CASE WHEN imdb_rating < 0 OR imdb_rating > 10 THEN 1 END) AS invalid_ratings,
            CASE 
                WHEN COUNT(CASE WHEN imdb_rating < 0 OR imdb_rating > 10 THEN 1 END) = 0 
                THEN 'PASS' 
                ELSE 'FAIL' 
            END AS status,
            'Expected: 0 <= rating <= 10' AS expected
        FROM RAW.MOVIES_RAW
        WHERE imdb_rating IS NOT NULL;
        
        -- แสดงรายการคะแนนที่ผิดปกติ (ถ้ามี)
        SELECT 
            '   Invalid IMDb Ratings:' AS detail,
            rank, title, imdb_rating
        FROM RAW.MOVIES_RAW
        WHERE imdb_rating < 0 OR imdb_rating > 10
        ORDER BY imdb_rating;
        
        -- --------------------------------------------------------------------
        -- 7. ตรวจสอบความยาวหนัง Runtime (ไม่ควรติดลบหรือเป็น 0)
        -- --------------------------------------------------------------------
        SELECT 
            '7. Runtime Validation' AS check_name,
            ROUND(MIN(runtime_mins), 0) AS min_runtime,
            ROUND(MAX(runtime_mins), 0) AS max_runtime,
            ROUND(AVG(runtime_mins), 0) AS avg_runtime,
            COUNT(CASE WHEN runtime_mins <= 0 THEN 1 END) AS invalid_runtime,
            CASE 
                WHEN COUNT(CASE WHEN runtime_mins <= 0 THEN 1 END) = 0 
                THEN 'PASS' 
                ELSE 'FAIL' 
            END AS status,
            'Expected: runtime > 0' AS expected
        FROM RAW.MOVIES_RAW
        WHERE runtime_mins IS NOT NULL;
        
        -- แสดงรายการความยาวหนังที่ผิดปกติ (ถ้ามี)
        SELECT 
            '   Invalid Runtime:' AS detail,
            rank, title, runtime_mins
        FROM RAW.MOVIES_RAW
        WHERE runtime_mins <= 0
        ORDER BY runtime_mins;
        
        -- --------------------------------------------------------------------
        -- 8. ตรวจสอบคะแนน Rotten Tomatoes (ต้องอยู่ระหว่าง 0-100)
        -- --------------------------------------------------------------------
        SELECT 
            '8. Rotten Tomatoes Validation' AS check_name,
            ROUND(MIN(rotten_tomatoes_pct), 2) AS min_score,
            ROUND(MAX(rotten_tomatoes_pct), 2) AS max_score,
            ROUND(AVG(rotten_tomatoes_pct), 2) AS avg_score,
            COUNT(CASE WHEN rotten_tomatoes_pct < 0 OR rotten_tomatoes_pct > 100 THEN 1 END) AS invalid_scores,
            CASE 
                WHEN COUNT(CASE WHEN rotten_tomatoes_pct < 0 OR rotten_tomatoes_pct > 100 THEN 1 END) = 0 
                THEN 'PASS' 
                ELSE 'FAIL' 
            END AS status,
            'Expected: 0 <= score <= 100' AS expected
        FROM RAW.MOVIES_RAW
        WHERE rotten_tomatoes_pct IS NOT NULL;
        
        -- แสดงรายการคะแนนที่ผิดปกติ (ถ้ามี)
        SELECT 
            '   Invalid RT Scores:' AS detail,
            rank, title, rotten_tomatoes_pct
        FROM RAW.MOVIES_RAW
        WHERE rotten_tomatoes_pct < 0 OR rotten_tomatoes_pct > 100
        ORDER BY rotten_tomatoes_pct;
        
        -- --------------------------------------------------------------------
        -- 9. ตรวจสอบจำนวนรางวัลออสการ์ (ไม่ควรติดลบ)
        -- --------------------------------------------------------------------
        SELECT 
            '9. Oscars Won Validation' AS check_name,
            MIN(oscars_won) AS min_oscars,
            MAX(oscars_won) AS max_oscars,
            ROUND(AVG(oscars_won), 1) AS avg_oscars,
            COUNT(CASE WHEN oscars_won < 0 THEN 1 END) AS invalid_oscars,
            CASE 
                WHEN COUNT(CASE WHEN oscars_won < 0 THEN 1 END) = 0 
                THEN 'PASS' 
                ELSE 'FAIL' 
            END AS status,
            'Expected: oscars >= 0' AS expected
        FROM RAW.MOVIES_RAW
        WHERE oscars_won IS NOT NULL;
        
        -- แสดงรายการจำนวนออสการ์ที่ผิดปกติ (ถ้ามี)
        SELECT 
            '   Invalid Oscars Count:' AS detail,
            rank, title, oscars_won
        FROM RAW.MOVIES_RAW
        WHERE oscars_won < 0
        ORDER BY oscars_won;
        
        -- --------------------------------------------------------------------
        -- 10. ตรวจสอบคะแนน Metacritic (ต้องอยู่ระหว่าง 0-100)
        -- --------------------------------------------------------------------
        SELECT 
            '10. Metacritic Score Validation' AS check_name,
            ROUND(MIN(metacritic_score), 2) AS min_score,
            ROUND(MAX(metacritic_score), 2) AS max_score,
            ROUND(AVG(metacritic_score), 2) AS avg_score,
            COUNT(CASE WHEN metacritic_score < 0 OR metacritic_score > 100 THEN 1 END) AS invalid_scores,
            CASE 
                WHEN COUNT(CASE WHEN metacritic_score < 0 OR metacritic_score > 100 THEN 1 END) = 0 
                THEN 'PASS' 
                ELSE 'FAIL' 
            END AS status,
            'Expected: 0 <= score <= 100' AS expected
        FROM RAW.MOVIES_RAW
        WHERE metacritic_score IS NOT NULL;
        
        -- แสดงรายการคะแนนที่ผิดปกติ (ถ้ามี)
        SELECT 
            '   Invalid Metacritic Scores:' AS detail,
            rank, title, metacritic_score
        FROM RAW.MOVIES_RAW
        WHERE metacritic_score < 0 OR metacritic_score > 100
        ORDER BY metacritic_score;
        
        -- --------------------------------------------------------------------
        -- 11. ตรวจสอบ Box Office (ไม่ควรติดลบ)
        -- --------------------------------------------------------------------
        SELECT 
            '11. Box Office Validation' AS check_name,
            ROUND(MIN(box_office_millions), 2) AS min_revenue,
            ROUND(MAX(box_office_millions), 2) AS max_revenue,
            ROUND(AVG(box_office_millions), 2) AS avg_revenue,
            COUNT(CASE WHEN box_office_millions < 0 THEN 1 END) AS invalid_revenue,
            CASE 
                WHEN COUNT(CASE WHEN box_office_millions < 0 THEN 1 END) = 0 
                THEN 'PASS' 
                ELSE 'FAIL' 
            END AS status,
            'Expected: revenue >= 0' AS expected
        FROM RAW.MOVIES_RAW
        WHERE box_office_millions IS NOT NULL;
        
        -- ====================================================================
        -- SUMMARY STATISTICS
        -- ====================================================================
        SELECT '=== DATA SUMMARY STATISTICS ===' AS report_section;
        
        SELECT 
            COUNT(*) AS total_rows,
            COUNT(DISTINCT title) AS unique_titles,
            MIN(year) AS earliest_year,
            MAX(year) AS latest_year,
            ROUND(AVG(imdb_rating), 2) AS avg_imdb_rating,
            ROUND(AVG(rotten_tomatoes_pct), 2) AS avg_rt_score,
            ROUND(AVG(metacritic_score), 2) AS avg_metacritic,
            ROUND(AVG(runtime_mins), 0) AS avg_runtime_mins,
            ROUND(SUM(box_office_millions), 2) AS total_box_office_millions
        FROM RAW.MOVIES_RAW;
        
        -- ====================================================================
        -- NULL VALUE ANALYSIS
        -- ====================================================================
        SELECT '=== NULL VALUE ANALYSIS ===' AS report_section;
        
        SELECT 
            'NULL Count per Column' AS analysis_type,
            COUNT(CASE WHEN rank IS NULL THEN 1 END) AS null_rank,
            COUNT(CASE WHEN title IS NULL THEN 1 END) AS null_title,
            COUNT(CASE WHEN year IS NULL THEN 1 END) AS null_year,
            COUNT(CASE WHEN imdb_rating IS NULL THEN 1 END) AS null_imdb,
            COUNT(CASE WHEN rotten_tomatoes_pct IS NULL THEN 1 END) AS null_rt,
            COUNT(CASE WHEN runtime_mins IS NULL THEN 1 END) AS null_runtime,
            COUNT(CASE WHEN oscars_won IS NULL THEN 1 END) AS null_oscars,
            COUNT(CASE WHEN box_office_millions IS NULL THEN 1 END) AS null_box_office,
            COUNT(CASE WHEN metacritic_score IS NULL THEN 1 END) AS null_metacritic
        FROM RAW.MOVIES_RAW;
        
        -- ====================================================================
        -- SAMPLE DATA (First 5 rows)
        -- ====================================================================
        SELECT '=== SAMPLE DATA (Top 5 Movies) ===' AS report_section;
        
        SELECT 
            rank,
            title,
            year,
            director,
            imdb_rating,
            rotten_tomatoes_pct,
            runtime_mins,
            oscars_won
        FROM RAW.MOVIES_RAW
        ORDER BY rank
        LIMIT 5;
        
        -- ====================================================================
        -- FINAL STATUS
        -- ====================================================================
        SELECT '=== FINAL VALIDATION STATUS ===' AS report_section;
        
        SELECT 
            CASE 
                WHEN (
                    SELECT COUNT(*) FROM RAW.MOVIES_RAW 
                    WHERE year < 1800 OR year > 2025
                       OR (imdb_rating < 0 OR imdb_rating > 10)
                       OR runtime_mins <= 0
                       OR (rotten_tomatoes_pct < 0 OR rotten_tomatoes_pct > 100)
                       OR oscars_won < 0
                       OR (metacritic_score < 0 OR metacritic_score > 100)
                       OR box_office_millions < 0
                ) = 0 
                THEN 'ALL CHECKS PASSED - Data quality is GOOD'
                ELSE 'SOME CHECKS FAILED - Please review the validation details above'
            END AS final_status;
        """,
        dag=None,
    )
    
    # ========================================================================
    # Define Task Dependencies
    # ========================================================================
    upload_result = upload_to_s3()
    verify_result = verify_upload(upload_result)
    
    # Chain: verify → create_schema → create_table → load_data → verify_data
    verify_result >> create_schema >> create_table >> load_data >> verify_data


# ============================================================================
# Instantiate DAG
# ============================================================================
movies_dag = movies_data_pipeline()