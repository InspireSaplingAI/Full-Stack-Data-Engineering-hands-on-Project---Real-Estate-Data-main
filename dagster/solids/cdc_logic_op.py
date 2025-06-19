from dagster import op
from scripts.cdc_logic import cdc_logic

@op
def cdc_logic_op(context, csv_path):
    return cdc_logic(csv_path)