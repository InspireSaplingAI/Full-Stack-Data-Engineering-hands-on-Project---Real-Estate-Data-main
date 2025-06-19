from dagster import graph, DynamicOut, DynamicOutput, op, Config
from dagster import ConfigurableResource

from dagster.solids.scrape_real_estate_op import scrape_real_estate_op
from dagster.solids.cdc_logic_op import cdc_logic_op
from dagster.solids.upload_to_s3_op import upload_to_s3_op
from dagster.solids.create_delta_table_op import create_delta_table_op
from dagster.solids.ingest_to_druid_op import ingest_to_druid_op

class CityConfig(Config):
    cities: list
    state: str

@op(out=DynamicOut())
def emit_cities(context, config: CityConfig):
    for city in config.cities:
        yield DynamicOutput(city, mapping_key=city)

@graph
def real_estate_pipeline():
    cities = emit_cities()
    raw_paths = cities.map(scrape_real_estate_op)
    filtered_data = raw_paths.map(cdc_logic_op)
    s3_paths = filtered_data.map(lambda x: upload_to_s3_op(x[0]))
    druid_results = filtered_data.map(lambda x: ingest_to_druid_op(x[1]))
    s3_paths_collected = s3_paths.collect()
    create_delta_table_op()
    
