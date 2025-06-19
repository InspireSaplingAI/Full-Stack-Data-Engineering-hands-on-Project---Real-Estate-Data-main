from deltalake import DeltaTable, write_deltalake
from dagster import OpExecutionContext
import pandas as pd
import pyarrow as pa
from typing import Dict, Tuple

DeltaCoordinate = Dict[str, str]

def reading_delta_table(context: OpExecutionContext, path: str) -> Tuple[pd.DataFrame, DeltaTable]:
    """
    TODO: Read an existing Delta table from the given S3 path.

    If it doesn't exist yet, return an empty DataFrame and None.
    """
    # Example:
    # if DeltaTable.exists(path):
    #     dt = DeltaTable(path)
    #     df = dt.to_pandas()
    #     return df, dt
    # else:
    #     context.log.warning("Table not found, returning empty")
    #     return pd.DataFrame(), None
    pass


def merge_property_delta(context: OpExecutionContext, input_dataframe: pd.DataFrame) -> DeltaCoordinate:
    """
    TODO: Merge incoming property data into the Delta table based on propertyDetails_propertyId.

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
    target_delta_table = "s3a://real-estate/lake/bronze/property"
    target_delta_coordinate = {
        "s3_coordinate_bucket": "real-estate",
        "s3_coordinate_key": "lake/bronze/property",
        "table_name": "property",
        "database": "core"
    }

    # ✅ Step 2: Read existing Delta table
    # existing_df, dt = reading_delta_table(context, target_delta_table)

    # ✅ Step 3: Log the schema of the input dataframe
    # input_table_pa = pa.Table.from_pandas(input_dataframe)
    # context.log.debug(f"Input DataFrame schema: {input_table_pa.schema}")

    # ✅ Step 4a: If table doesn't exist, write new Delta table
    # if dt is None:
    #     write_deltalake(target_delta_table, input_dataframe, mode="overwrite")
    #     context.log.info("Created new Delta table.")
    # else:
    # ✅ Step 4b: Merge input dataframe into existing Delta table
    #     dt.merge(
    #         source=input_dataframe,
    #         predicate='target.propertyDetails_propertyId = source."propertyDetails_propertyId"',
    #         source_alias='source',
    #         target_alias='target'
    #     ).when_matched_update_all() \
    #      .when_not_matched_insert_all() \
    #      .execute()
    #     context.log.info("Merged data into Delta table.")

    # ✅ Step 5: Return metadata
    # return target_delta_coordinate
    pass

def create_delta_table():
    ###
    # to do: read from S3 all incoming data (parqeut file) and merge_property_delta for all the dataframe 
    # reading from parquet
    # how to read multiple parquet files in parrele and update the same table in delta lake?
    
    ###
