import json
import requests
import pandas as pd
from dagster import OpExecutionContext
from typing import List, Dict

# Type alias for housing records
HousingData = List[Dict]

def ingest_to_druid(context: OpExecutionContext, housing_data: HousingData, druid_url: str) -> None:
    """
    Ingest new housing data (with prices) into Druid via HTTP streaming.

    Args:
        context: Dagster context object for logging.
        housing_data: List of new housing data dicts (including price).
        druid_url: The Druid endpoint URL for streaming ingestion (e.g., http://localhost:8200/druid/indexer/v1/task)
    """

    # ✅ Step 1: Log how many records you're sending
    context.log.info(f"Sending {len(housing_data)} records to Druid.")

    # ✅ Step 2: Convert the housing data to newline-delimited JSON (NDJSON)
    # Example: '{"id": 1, "price": 500000}\n{"id": 2, "price": 520000}'
    ndjson_data = "\n".join([json.dumps(record) for record in housing_data])

    # ✅ Step 3: Set appropriate headers
    headers = {
        "Content-Type": "application/x-ndjson"
    }

    # ✅ Step 4: Send POST request to Druid streaming endpoint
    try:
        response = requests.post(druid_url, data=ndjson_data, headers=headers)
        if response.status_code == 200:
            context.log.info("✅ Successfully streamed data to Druid.")
        else:
            context.log.error(f"🛑 Failed to ingest data: {response.status_code} - {response.text}")
    except Exception as e:
        context.log.error(f"🛑 Error while streaming to Druid: {str(e)}")

   
