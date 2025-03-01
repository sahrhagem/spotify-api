import os
import boto3
from datetime import datetime
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import json
import pytz

# Load environment variables
load_dotenv()


# MinIO Configuration
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')
BUCKET_NAME = os.getenv('BUCKET_NAME')
STREAM_DIR = os.getenv('STREAM_DIR')


# Connect to MinIO
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

def file_exists(bucket, key):
    """Check if a file exists in MinIO."""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True  # File exists
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False  # File does not exist
        else:
            raise  # Some other error occurred

def remove_key_recursively(data, target_key):
    """ Recursively remove all occurrences of target_key from the JSON data. """
    if isinstance(data, dict):
        return {k: remove_key_recursively(v, target_key) for k, v in data.items() if k != target_key}
    elif isinstance(data, list):
        return [remove_key_recursively(item, target_key) for item in data]
    else:
        return data


def process_json(data):
    # Define timezone
    utc = pytz.utc
    berlin_tz = pytz.timezone('Europe/Berlin')

    # Extract and convert timestamp
    utc_time = datetime.strptime(data['played_at'], "%Y-%m-%dT%H:%M:%S.%fZ")  # Handle milliseconds
    utc_time = utc.localize(utc_time)
    berlin_time = utc_time.astimezone(berlin_tz)

    data['played_at'] = berlin_time.strftime('%Y-%m-%dT%H:%M:%S')  # Remove milliseconds

    # Remove target key recursively
    data = remove_key_recursively(data, "available_markets")
    return data



def upload_file_to_minio(file_path):
    """Uploads a file to MinIO only if it does not already exist."""
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        data = process_json(data)
        time_stamp = datetime.strptime(data['played_at'], "%Y-%m-%dT%H:%M:%S")

        # Get current timestamp for folder structure
        year, month, day, hour = time_stamp.strftime("%Y"), time_stamp.strftime("%m"), time_stamp.strftime("%d"), time_stamp.strftime("%H")

        # Format filename with Berlin timestamp
        filename = time_stamp.strftime('%Y-%m-%dT%H_%M_%S.json')

        # Define destination path in MinIO (e.g., raw/year=2025/month=02/day=28/hour=14/stream_123456.json)
        minio_path = f"streams/raw/year={year}/month={month}/day={day}/{filename}"

        filepath_processed = f"{file_path}_berlin"

        # Write new JSON file
        with open(filepath_processed, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)  # Pretty-print with indentation

        # Upload the file directly
        with open(filepath_processed, "rb") as file_data:
            s3.put_object(Bucket=BUCKET_NAME, Key=minio_path, Body=file_data, ContentType="application/json")

        print(f"Uploaded: {file_path} → {minio_path}")


    except Exception as e:
        print(f"Error processing {file_path}: {e}")

directory = STREAM_DIR
for filename in os.listdir(directory):
    if filename.endswith(".json"):
        filepath = os.path.join(directory, filename)
        upload_file_to_minio(filepath)


# List all objects in the bucket
# response = s3.list_objects_v2(Bucket=BUCKET_NAME)

# if 'Contents' in response:
#     for obj in response['Contents']:
#         print(obj['Key'])  # Print file paths
# else:
#     print("❌ No files found in the bucket.")
