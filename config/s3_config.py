import os

S3_CONFIG = {
    "S3_ENDPOINT_URL": os.getenv("S3_ENDPOINT_URL"),
    "S3_ACCESS_KEY_ID": os.getenv("S3_ACCESS_KEY_ID"),
    "S3_SECRET_ACCESS_KEY": os.getenv("S3_SECRET_ACCESS_KEY"),
    "S3_BUCKET_NAME": os.getenv("S3_BUCKET_NAME"),
}