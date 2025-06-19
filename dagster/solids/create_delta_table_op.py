from dagster import op
from scripts.create_delta_table import create_delta_table

@op
def create_delta_table_op(context):
    return create_delta_table()