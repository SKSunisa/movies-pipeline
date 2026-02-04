"""
Upload CSV file to AWS S3 bucket.
Airflow and Docker compatible version.
"""

import os
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv 
import platform

# Load environment variables from .env file
load_dotenv()

# ====================================
#  Upload CSV file to S3 bucket
# ====================================
def upload_to_s3(add_timestamp=False):

    # ==========================================
    # Get Environment Variables
    # ==========================================
    # S3 configuration
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    s3_bucket = os.getenv("S3_BUCKET_NAME")

    # ==========================================
    # Validate Environment Variables
    # ==========================================
    # Check if all required environment variables are set 
    missing_vars = []
    if not aws_access_key:
        missing_vars.append("AWS_ACCESS_KEY_ID")
    if not aws_secret_key:
        missing_vars.append("AWS_SECRET_ACCESS_KEY")
    if not s3_bucket:
        missing_vars.append("S3_BUCKET_NAME")
    if missing_vars:
        print("Error: Missing required environment variables")
        print("Make sure your .env file contains:")
        print(" - AWS_ACCESS_KEY_ID")
        print(" - AWS_SECRET_ACCESS_KEY")
        print(" - S3_BUCKET_NAME")
        print(" - AWS_REGION (optional, defaults to us-east-1)")
        raise ValueError(
            f"Missing environment variables: {', '.join(missing_vars)}"
        )
    
    # ==========================================
    # Upload File to S3
    # ==========================================
    
    # Path inside Airflow container
    if platform.system() == 'Windows':
    # รัน local บน Windows
        local_file = "data/top_100_movies_full_best_effort.csv"
    else:
    # รันใน Docker container
        local_file = "/opt/airflow/data/top_100_movies_full_best_effort.csv"

     # S3 key with timestamp
    if add_timestamp:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        s3_key = f"raw/top_100_movies_{timestamp}.csv" 
    else:                                               
        s3_key = "raw/top_100_movies_full_best_effort.csv"  

    # ==========================================
    # Validate File Exists
    # ==========================================
    if not os.path.exists(local_file):
        print(f"ERROR: File not found: {local_file}")
        print("Expected file location: /opt/airflow/data/")
        print("Make sure the CSV file is in the data/ folder")
        raise FileNotFoundError(f"File not found: {local_file}")
    
    # Get file size
    file_size = os.path.getsize(local_file)
    file_size_kb = file_size / 1024


    # ==========================================
    # Display Upload Info
    # ==========================================
    print("Upload Configuration:")
    print(f"Source File    : {local_file}")
    print(f"File Size      : {file_size_kb:.2f} KB ({file_size:,} bytes)")
    print(f"S3 Bucket      : {s3_bucket}")
    print(f"S3 Key         : {s3_key}")
    print(f"AWS Region     : {aws_region}")
    print(f"Full S3 Path   : s3://{s3_bucket}/{s3_key}")


    # ==========================================
    # Upload to S3
    # ==========================================
    print(" Upload to S3")
    print(" Starting upload...")

    try:
        # Create S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=aws_region
        )
        
        # Upload file with progress
        print(" Uploading file to S3...")
        s3_client.upload_file(
            local_file,
            s3_bucket,
            s3_key,
            ExtraArgs={"ContentType": "text/csv"}
        )
        
        # ==========================================
        # Verify Upload
        # ==========================================
        print(" Verifying upload...")
        response = s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
        s3_file_size = response['ContentLength']
        
        if s3_file_size == file_size:
            print(" File size verified!")
        else:
            print(f" Warning: Size mismatch (local: {file_size}, S3: {s3_file_size})")
        
        # ==========================================
        # Success
        # ==========================================
        print(" SUCCESS File uploaded to S3")
        print(f" S3 URI   : s3://{s3_bucket}/{s3_key}")
        print(f" Size     : {file_size_kb:.2f} KB")
        print(f" Timestamp : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
        
        # Return result for Airflow XCom
        return {
            "s3_bucket": s3_bucket,
            "s3_key": s3_key,
            "s3_uri": f"s3://{s3_bucket}/{s3_key}",
            "file_size": file_size,
            "file_size_kb": file_size_kb,
            "upload_timestamp": datetime.now().isoformat()
        }
        
    except ClientError as e:
        # ==========================================
        # AWS Error
        # ==========================================
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        
        print(" AWS ERROR!")
        print(f" Error Code : {error_code}")
        print(f" Message    : {error_message}")
        print("\n Common causes:")
        print(" 1. Invalid AWS credentials")
        print(" 2. Bucket doesn't exist")
        print(" 3. No permission to write to bucket")
        print(" 4. Incorrect AWS region")
        raise
        
    except Exception as e:
        # ==========================================
        # Unexpected Error
        # ==========================================
        print(" UNEXPECTED ERROR!")
        print(f" Error : {str(e)}")
        print(f" Type  : {type(e).__name__}")
        raise

# ==========================================
# List files in S3 bucket
# ==========================================
def list_s3_files(prefix='raw/'):
    print(f"\n Listing files in S3 (prefix: {prefix})...")

    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    s3_bucket = os.getenv('S3_BUCKET_NAME')

    try:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=aws_region
        )
        
        response = s3_client.list_objects_v2(
            Bucket=s3_bucket,
            Prefix=prefix
        )
        
        if 'Contents' in response:
            # Filter out folder markers (files ending with /)
            files = [obj for obj in response['Contents'] if not obj['Key'].endswith('/')]
        
            print(f"\n Found {len(files)} files:")
            for obj in files:
                size_kb = obj["Size"] / 1024
                print(f"  - {obj['Key']} ({size_kb:.2f} KB)")
            return files
        else:
            print("No files found")
            return []
            
    except Exception as e:
        print(f" Error listing files: {str(e)}")
        raise

if __name__ == "__main__":
    print("Running upload script directly...")
    result = upload_to_s3(add_timestamp=False)
    print(f"\nResult: {result}")
    
    # List files
    list_s3_files()



    
