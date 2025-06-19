from dagster import op
from scripts.ingest_to_druid import ingest_to_druid

@op
def ingest_to_druid_op(context):
    return ingest_to_druid()