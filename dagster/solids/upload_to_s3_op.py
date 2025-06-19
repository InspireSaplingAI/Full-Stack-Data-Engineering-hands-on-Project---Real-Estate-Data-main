from dagster import op
from scripts.upload_to_s3 import upload_to_s3

@op
def upload_to_s3_op(context, filtered_path, city, state):
    return upload_to_s3(filtered_path, city, state)