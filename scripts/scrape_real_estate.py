from bs4 import BeautifulSoup
import requests
from csv import writer

def scrape_real_estate_data(city, state):
    url = f"https://www.realtor.com/realestateandhomes-search/{city}_{state}/show-newest-listings"

    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36",
    "Accept-Encoding": "gzip, deflate", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "DNT": "1", "Connection": "close", "Upgrade-Insecure-Requests": "1"}

    page = requests.get(url, headers=headers)
    soup = BeautifulSoup(page.text, 'html.parser')

    raw_file_path = f"data/raw/{city}_{state}_real_estate.csv"
    ###
    # to do: Extract property details from the soup object
    # save dataframe to csv file
    # Add datetime_collected column to incoming data
    ###

    
    return raw_file_path