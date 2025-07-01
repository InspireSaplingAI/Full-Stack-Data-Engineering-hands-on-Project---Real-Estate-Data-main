###
# Change Data Capture (CDC) logic function, implemented to yield only new or changed properties by comparing a properties list against a stored property_table.
###

import pandas as pd
import pandasql as ps
from typing import List, Dict
import os
from datetime import datetime
from typing import Tuple


# Type alias for clarity
PropertyDataFrame = List[Dict]

def cdc_logic(csv_file_path: str) -> Tuple[str, List[Dict]]:
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
    processed_path = os.path.join('data/processed', filename.replace('.csv', '.parquet'))
    # Ensure the processed directory exists
    os.makedirs(os.path.dirname(processed_path), exist_ok=True)

    # If processed file does not exist, treat all as new
    if not os.path.exists(processed_path):
        incoming_df.to_parquet(processed_path, engine='pyarrow', index=False)
        changed_props = incoming_df.to_dict(orient="records")
        # If no processed file exists, all incoming properties are considered new
        return (processed_path, changed_props)

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
    if properties.empty:
        return []

    ###
    # Implement CDC logic using pandasql to compare incoming properties against existing ones
    # Only consider change if Price or Status is different for a given Location

    # Ensure columns are consistent and handle missing columns gracefully
    required_cols = ['Location', 'Status', 'Price']
    for col in required_cols:
        if col not in properties.columns or col not in property_table.columns:
            raise ValueError(f"Missing required column: {col}")

    # Use pandasql to find new or changed properties
    query = """
        SELECT p.*
        FROM properties p
        LEFT JOIN property_table e
            ON p.Location = e.Location
        WHERE e.Location IS NULL
           OR p.Price != e.Price
           OR p.Status != e.Status
    """
    df_changed = ps.sqldf(query, locals())
    # Query the existing `property_table` to extract existing fingerprints
    # You can use this example SQL query:
    # Query the existing `property_table` to extract existing fingerprints
    # Example (not used in main logic, but for reference):
    # query = """
    #     SELECT Location,
    #            CAST(Location AS TEXT) || '-' || CAST(Price AS TEXT) AS fingerprint
    #     FROM property_table
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

-if __name__ == "__main__":
    cdc_logic("data/raw/Stockton_CA_real_estate.csv")