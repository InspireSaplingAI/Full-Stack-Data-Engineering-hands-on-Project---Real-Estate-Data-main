import sys
from deltalake import DeltaTable, write_deltalake
from dagster import OpExecutionContext
import pandas as pd
import pyarrow as pa
from typing import Dict, Tuple
import boto3
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.s3_config import S3_CONFIG
import s3fs

DeltaCoordinate = Dict[str, str]

def reading_delta_table(context: OpExecutionContext, path: str, fs: s3fs.S3FileSystem) -> Tuple[pd.DataFrame, DeltaTable]:
    """
    Read an existing Delta table from the given S3 path.

    If it doesn't exist yet, return an empty DataFrame and None.
    """
    try:
        # Check if the Delta table path exists using s3fs
        s3_path = path.replace("s3a://", "s3://")
        if fs.exists(s3_path):
            dt = DeltaTable(path)
            df = dt.to_pandas()
            return df, dt
        else:
            if context:
                context.log.warning("Delta table not found at path: %s, returning empty DataFrame.", path)
            return pd.DataFrame(), None
    except Exception as e:
        if context:
            context.log.warning(f"Error reading Delta table at {path}: {e}. Returning empty DataFrame.")
        return pd.DataFrame(), None


def merge_property_delta(context: OpExecutionContext, input_dataframe: pd.DataFrame, fs: s3fs.S3FileSystem) -> DeltaCoordinate:
    """
    Merge incoming property data into the Delta table based on propertyDetails_propertyId.

    Steps:
    1. Define the Delta table path and metadata.
    2. Read the existing Delta table if it exists.
    3. If the table doesn't exist, write the input dataframe as a new Delta table.
    4. If it does exist, merge new data using Delta Lake's merge logic:
        - Match rows on `propertyDetails_propertyId`
        - If matched, update all columns
        - If not matched, insert new rows
    5. Return Delta table metadata for downstream jobs.
    """

    # ✅ Step 1: Set Delta table path and return metadata dictionary
    target_delta_table = "s3a://real-estate/delta-lake/property"
    target_delta_coordinate = {
        "s3_coordinate_bucket": "real-estate",
        "s3_coordinate_key": "delta-lake/property",
        "table_name": "property",
        "database": "core"
    }

    # ✅ Step 2: Read existing Delta table
    existing_df, dt = reading_delta_table(context, target_delta_table, fs)

    # ✅ Step 3: Log the schema of the input dataframe
    input_table_pa = pa.Table.from_pandas(input_dataframe)
    if context:
        context.log.debug(f"Input DataFrame schema: {input_table_pa.schema}")

    # ✅ Step 4a: If table doesn't exist, write new Delta table
    if dt is None:
        write_deltalake(target_delta_table, input_dataframe, mode="overwrite")
        if context:
            context.log.info("Created new Delta table.")
    else:
        # ✅ Step 4b: Merge input dataframe into existing Delta table
        dt.merge(
            source=input_dataframe,
            predicate='target.propertyDetails_propertyId = source."propertyDetails_propertyId"',
            source_alias='source',
            target_alias='target'
        ).when_matched_update_all() \
         .when_not_matched_insert_all() \
         .execute()
        if context:
            context.log.info("Merged data into Delta table.")

    # ✅ Step 5: Return metadata
    return target_delta_coordinate

def create_delta_table():
    ###
    # read from S3 all incoming data (parquet file) and merge_property_delta for all the dataframe
    # reading from parquet
    # how to read multiple parquet files in parallel and update the same table in delta lake?

    ###
    prefix = "new"
    s3_bucket = S3_CONFIG["S3_BUCKET_NAME"]
    minio_endpoint = S3_CONFIG["S3_ENDPOINT_URL"]
    access_key = S3_CONFIG["S3_ACCESS_KEY_ID"]
    secret_key = S3_CONFIG["S3_SECRET_ACCESS_KEY"]
    # Create boto3 client with credentials and optional endpoint
    s3 = boto3.client(
            "s3",
            endpoint_url=minio_endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=boto3.session.Config(signature_version="s3v4"),
            region_name="us-east-1"  # MinIO ignores region, but boto3 requires it
        )

    # List all parquet files under the prefix
    paginator = s3.get_paginator("list_objects_v2")
    import concurrent.futures

    def process_parquet_file(s3_path):
        # Read the parquet file into a DataFrame
        fs = s3fs.S3FileSystem(
            key=access_key,
            secret=secret_key,
            client_kwargs={"endpoint_url": minio_endpoint}
        )
        with fs.open(s3_path.replace("s3a://", "s3://"), "rb") as f:
            df = pd.read_parquet(f, engine="pyarrow")
        # Merge into Delta table
        merge_property_delta(None, df, fs)  # Replace None with a Dagster context if available

    parquet_files = []
    for page in paginator.paginate(Bucket=s3_bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                parquet_files.append(f"s3a://{s3_bucket}/{obj['Key']}")

    print(f"Processing parquet files: {parquet_files}")

    # Process all parquet files in parallel
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(process_parquet_file, parquet_files)


if __name__ == "__main__":
    # This is just for testing the script locally
    # In production, this function would be called by a Dagster job
    create_delta_table()
    print("Delta table creation and merging completed.")