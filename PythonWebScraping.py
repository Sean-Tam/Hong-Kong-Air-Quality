# Topic: Data visualization and analysis of air pollution in Hong Kong

# -------------------- Required Libraries --------------------
import os
import numpy as np
import pandas as pd
from datetime import datetime

# Suppress warnings
import warnings
warnings.filterwarnings('ignore')


# -------------------- Data Web Scraping --------------------

# Required Libraries
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

# Path to ChromeDriver executable
chrome_driver_path = 'chromedriver-win64/chromedriver.exe'  # Update this path

# Configure Chrome options (optional)
options = Options()
options.headless = True  # Run in headless mode (without opening a browser window)
options.add_argument('--disable-gpu')  # Disable GPU acceleration (useful for some environments)

# Create a Service object for ChromeDriver
service = Service(chrome_driver_path)

# Initialize the WebDriver with the Service object and options
driver = webdriver.Chrome(service=service, options=options)

# Navigate to the website
url = 'https://www.aqhi.gov.hk/en/index.html'
driver.get(url)

# Locate and click the "List" button
list_button_div = driver.find_element(By.CLASS_NAME, 'icon-table')
list_button = list_button_div.find_element(By.TAG_NAME, 'a')
list_button.click()

# Locate the time data
time_data_div = driver.find_element(By.CLASS_NAME, 'myTimeInTable')

# Extract information from the page using BeautifulSoup (optional)
from bs4 import BeautifulSoup
soup = BeautifulSoup(driver.page_source, 'html.parser')
table_rows = soup.find_all('tr')
time_rows = soup.find_all('div', class_='myTimeInTable')

# Close the WebDriver
driver.quit()


# -------------------- Data Cleansing --------------------

# Select data from web scrapped raw data
web_scrapped_data_from_EPD = table_rows[458:]

# Cleanse the raw data and store it into a list
data_from_EPD = []
for i in range(len(web_scrapped_data_from_EPD)):
    data_from_EPD.append(web_scrapped_data_from_EPD[i].text)

# Initialize variables
final_data = []
current_station = []

# Process the data
for item in data_from_EPD:
    if item.startswith("Station Names"):  # Check if it's a new station name
        if current_station:  # Save the previous station data
            final_data.append(current_station)
        # Remove "Station Names" and the parentheses part
        current_station = [item.replace("Station Names", "").split("(")[0].strip()]
    else:
        current_station.append(item)

# Append the last station's data
if current_station:
    final_data.append(current_station)

# Cleanse the unnecessary data and store it into a list
cleansed_data = []
for station in final_data:
    cleansed_data.append([
        station[0],  # Station Name
        station[1].replace("NO2", ""),  # NO2
        station[2].replace("O3", ""),  # O3
        station[3].replace("SO2", ""),  # SO2
        station[4].replace("CO", ""),  # CO
        station[5].replace("PM10", ""),  # PM10
        station[6].replace("PM2.5", ""),  # PM2.5
        station[7].replace("AQHI", "")  # AQHI
    ])

# Turn the cleansed air pollutants data into DataFrame
data = pd.DataFrame(cleansed_data, columns = ['STATION', 'NO2', 'O3', 'SO2', 'CO', 'RSP', 'FSP', 'AQHI'])

# Extract date and time information
date_time_str = (time_rows[0].text)[4:-1]  # Remove '(At ' from the beginning and ')' from the end
date_time_obj = datetime.strptime(date_time_str, '%B %d, %Y %H:%M')

# Format the date and hour
date_str = date_time_obj.strftime('%d/%m/%Y')  # Day/Month/Year
hour = date_time_obj.hour + 1  # Adjusting the hour

# Add the time data into the dataset
data['DATE'] = date_str
data['HOUR'] = hour

# Replace all "-" values into None
data.replace("-", None, inplace=True)

# Rearrange and sort the data
data = data.loc[:, ['DATE', 'HOUR', 'STATION', 'NO2', 'O3', 'SO2', 'CO', 'RSP', 'FSP', 'AQHI']]
data = data.sort_values(by = ['DATE', 'HOUR', 'STATION'], ignore_index = True)

# -------------------- Storing the data in MongoDB Atlas --------------------

# Required Libraries
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# The url to connect MongoDB
uri = "mongodb+srv://user1:admin@airpollution.ktxvlrh.mongodb.net/?retryWrites=true&w=majority&appName=AirPollution"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Asses to the database and collection
database = client["AirPollutants"]
collection = database["PollutantsAtTheMoment"]

# Convert DataFrame to list of dictionaries
data_to_insert = data.to_dict(orient="records")

# Insert cleaned data into MongoDB
collection.insert_many(data_to_insert)



