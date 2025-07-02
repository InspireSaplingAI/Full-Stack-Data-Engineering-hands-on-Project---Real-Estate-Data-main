import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.s3_config import S3_CONFIG

def save_to_s3_minio(
    data: bytes,
    bucket_name: str,
    object_key: str,
    minio_endpoint: str,
    access_key: str,
    secret_key: str,
    content_type: str = "application/octet-stream"
) -> None:
    """Saves data to an S3 bucket using MinIO.

    Args:
        data (bytes): The content to upload.
        bucket_name (str): The bucket name in MinIO.
        object_key (str): The key (path/filename) for the object in the bucket.
        minio_endpoint (str): The URL for the MinIO server (e.g., "http://localhost:9000").
        access_key (str): MinIO access key.
        secret_key (str): MinIO secret key.
        content_type (str): MIME type of the content. Default is binary.

    Raises:
        Exception: If upload fails.
    """
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=minio_endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=boto3.session.Config(signature_version="s3v4"),
            region_name="us-east-1"  # MinIO ignores region, but boto3 requires it
        )

        # Ensure the bucket exists (optional: remove if bucket is guaranteed to exist)
        try:
            s3_client.head_bucket(Bucket=bucket_name)
        except ClientError:
            s3_client.create_bucket(Bucket=bucket_name)

        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=data,
            ContentType=content_type,
        )
        print(f"✅ Successfully uploaded to s3://{bucket_name}/{object_key}")

    except NoCredentialsError:
        raise Exception("🛑 Invalid MinIO credentials")
    except Exception as e:
        raise Exception(f"🛑 Failed to upload to MinIO: {str(e)}")

def upload_to_s3(filtered_path, city, state):
    ###
    # to_do: take the filtered_path and upload it to MinIO S3 bucket
    # use config/s3_config.py
    ###
    with open(filtered_path, "rb") as f:
        data = f.read()

    s3_key = f"new/{city}/{state}/{filtered_path.split('/')[-1]}"

    save_to_s3_minio(
        data=data,
        bucket_name=S3_CONFIG["S3_BUCKET_NAME"],
        object_key=s3_key,
        minio_endpoint=S3_CONFIG["S3_ENDPOINT_URL"],
        access_key=S3_CONFIG["S3_ACCESS_KEY_ID"],
        secret_key=S3_CONFIG["S3_SECRET_ACCESS_KEY"],
        content_type="application/octet-stream"
    )

if __name__ == "__main__":
    upload_to_s3("data/processed/Stockton_CA_real_estate.parquet", "Stockton", "CA")