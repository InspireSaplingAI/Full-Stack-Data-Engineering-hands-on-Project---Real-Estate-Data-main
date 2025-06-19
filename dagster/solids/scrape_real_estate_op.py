from dagster import op, OpExecutionContext
from scripts.scrape_real_estate_data import scrape_real_estate_data

@op
def scrape_real_estate_op(context: OpExecutionContext, city: str, state: str) -> list:
    """Scrapes real estate listings for a given city."""
    context.log.info(f"Scraping housing data for city: {city}, state: {state}")
    return scrape_real_estate_data(city, state)
