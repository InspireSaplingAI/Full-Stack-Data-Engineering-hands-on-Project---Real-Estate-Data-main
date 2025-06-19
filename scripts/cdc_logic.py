###
# Change Data Capture (CDC) logic function, implemented to yield only new or changed properties by comparing a properties list against a stored property_table.
###

import pandas as pd
import pandasql as ps
from typing import List, Dict
import os
from datetime import datetime

# Type alias for clarity
PropertyDataFrame = List[Dict]
def cdc_logic(csv_file_path: str) -> (str, List[Dict]):
    """
    Reads the incoming scraped properties from a CSV file, compares with existing properties,
    and returns a list of new or changed properties.

    Args:
        csv_file_path: Path to the incoming scraped properties CSV (e.g., 'raw/xxx.csv').

    Returns:
        List of new or changed property dicts.
    """

    # Read incoming properties
    incoming_df = pd.read_csv(csv_file_path)

    # Derive the processed file path
    filename = os.path.basename(csv_file_path)
    processed_path = os.path.join('processed', filename)

    # If processed file does not exist, treat all as new
    if not os.path.exists(processed_path):
        return incoming_df.to_dict(orient="records")

    # Read existing properties
    existing_df = pd.read_parquet(processed_path)

    # Get new or changed properties
    changed_props = get_new_or_changed_properties(incoming_df, existing_df)
    if not changed_props:
        return None

    # Convert to DataFrame
    df_changed = pd.DataFrame(changed_props)

    # Save new/changed properties to the processed parquet file (replace mode)
    df_changed.to_parquet(processed_path, engine='pyarrow', index=False)

    return (processed_path, changed_props)


def get_new_or_changed_properties(
    properties: pd.DataFrame,
    property_table: pd.DataFrame
) -> PropertyDataFrame:
    """Returns a list of new or changed properties by comparing fingerprints.

    Args:
        properties: Incoming scraped properties.
        property_table: Existing properties (e.g., from DB).

    Returns:
        List of new or changed property dicts.
    """
    if not properties:
        return []

    ###
    # to do: Implement CDC logic to compare incoming properties against existing ones
    # Query the existing `property_table` to extract existing fingerprints
    # You can use this example SQL query:
    # query = f"""
    #     SELECT propertyDetails_propertyId,
    #            CAST(propertyDetails_propertyId AS STRING) || '-' || propertyDetails_normalizedPrice AS fingerprint
    #     FROM property_table
    #     WHERE propertyDetails_propertyId IN ({ids})
    # """
    # result_df = ps.sqldf(query, locals())

    # Convert both result_df and incoming properties into pandas DataFrames with consistent columns

    # Use SQL (or pandas merge) to find new or changed records
    # For example:
    # SELECT p.*
    # FROM pd_properties p
    # LEFT JOIN pd_existing_props e
    #     ON p.id = e.propertyDetails_propertyId
    # WHERE p.fingerprint != e.fingerprint OR e.fingerprint IS NULL
    ###

    # Convert the final result (df_changed) to parquet and return it
    return df_changed.to_dict(orient="records")
