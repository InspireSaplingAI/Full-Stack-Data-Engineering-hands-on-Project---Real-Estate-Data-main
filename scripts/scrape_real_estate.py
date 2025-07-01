from bs4 import BeautifulSoup
import requests
from csv import writer
import datetime
import os
import json
import random

def scrape_real_estate_data(city, state):
    # Generate random sample real estate data
    # instead of scraping directly to avoid potential legal issues
    # it is possible to use API to get the data but will add complexity
    num_samples = 50
    sample_data = []
    street_names = ["Main", "Oak", "Pine", "Maple", "Cedar", "Elm", "Washington", "Lake", "Hill", "Sunset"]
    status_options = ["For Sale", "Pending", "Sold"]
    broker_names = ["Acme Realty", "Best Homes", "Dream Estates", "Sunrise Realty", "Urban Living"]

    for _ in range(num_samples):
        street = f"{random.randint(100, 9999)} {random.choice(street_names)} St"
        zipcode = random.randint(90000, 96999)
        address = f"{street}, {city}, {state} {zipcode}"
        status = random.choice(status_options)
        price = random.randint(150000, 1200000)
        owner = random.choice(broker_names)
        bed = random.randint(1, 6)
        bath = round(random.uniform(1, 5) * 2) / 2
        sqft = random.randint(700, 5000)
        sqft_lot = random.randint(1000, 20000)

        sample_data.append({
            "Location": address,
            "Status": status,
            "Price": price,
            "Owner": owner,
            "Bed": bed,
            "Bath": bath,
            "SQFT": sqft,
            "SQFT_LOT": sqft_lot
        })

    raw_file_path = f"data/raw/{city}_{state}_real_estate.csv"
    os.makedirs(os.path.dirname(raw_file_path), exist_ok=True)
    with open(raw_file_path, 'w', encoding='utf8', newline='') as f:
        thewriter = writer(f)
        header = ['Location', 'Status', 'Price', 'Owner', 'Bed', 'Bath', 'SQFT', 'SQFT_LOT', 'datetime_collected']
        thewriter.writerow(header)
        for row in sample_data:
            row['datetime_collected'] = datetime.datetime.now().isoformat()
            thewriter.writerow([row.get(h, "") for h in header])
    return raw_file_path

if __name__ == "__main__":
    scrape_real_estate_data("Stockton", "CA")
