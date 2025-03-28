#!/usr/bin/env python
# coding: utf-8

# # <span style='color:Orange'>Topic<span>
# 
# ### Data visualization and analysis of air pollution in Hong Kong
# 

# # Required Libraries

# In[5]:


# Required Libraries
import os
import numpy as np
import pandas as pd
from datetime import datetime

# Suppress warnings
import warnings
warnings.filterwarnings('ignore')


# # Data Web Scraping

# In[6]:


# Required Libraries
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

# Path to your ChromeDriver executable
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


# # Data Cleansing

# In[7]:


# Select data from web scrapped raw data
web_scrapped_data_from_EPD = table_rows[458:]

# Cleanse the raw data and store it into a list
data_from_EPD = []
for i in range(len(web_scrapped_data_from_EPD)):
    data_from_EPD.append(web_scrapped_data_from_EPD[i].text)

data_from_EPD


# In[8]:


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
data


# In[9]:


# Extract date and time information
date_time_str = (time_rows[0].text)[4:-1]  # Remove '(At ' from the beginning and ')' from the end
date_time_obj = datetime.strptime(date_time_str, '%B %d, %Y %H:%M')

# Format the date and hour
date_str = date_time_obj.strftime('%d/%m/%Y')  # Day/Month/Year
hour = date_time_obj.hour + 1  # Adjusting the hour

# Add the time data into the dataset
data['DATE'] = date_str
data['HOUR'] = hour
data


# In[13]:


data.replace("-", None, inplace=True)
data


# In[14]:


# Rearrange and sort the data
data = data.loc[:, ['DATE', 'HOUR', 'STATION', 'NO2', 'O3', 'SO2', 'CO', 'RSP', 'FSP', 'AQHI']]
data = data.sort_values(by = ['DATE', 'HOUR', 'STATION'], ignore_index = True)
data


# # Linkage with MongoDB Atlas

# In[16]:


from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://user1:admin@airpollution.ktxvlrh.mongodb.net/?retryWrites=true&w=majority&appName=AirPollution"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)


# In[17]:


# Check all database in the client
print(client.list_database_names())


# In[19]:


# Convert DataFrame to list of dictionaries
data_to_insert = data.to_dict(orient="records")

# Asses to the database and collection
database = client["AirPollutants"]
collection = database["PollutantsAtTheMoment"]

# Insert cleaned data into MongoDB
try:    
    collection.insert_many(data_to_insert)
    print("Cleaned data inserted successfully!")
except Exception as e:
    print(e)


# In[24]:


df = pd.DataFrame(list(collection.find()))
df.drop(columns=["_id"], inplace=True)
df


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# # Remarks

# ## Remarks from Hong Kong Environmental Protection Department(HKEPD):
# In the dataset:
# 
# 1. All Pollutant unit in μg/m3 except CO which is in 10μg/m3
# 2. N.A. = data not available 
# 3. CO = Carbon Monoxide
# 4. FSP = Fine Suspended Particulates (PM2.5)
# 5. NO2 = Nitrogen Dioxide
# 6. NOX = Nitrogen Oxides
# 7. O3 = Ozone
# 8. RSP = Respirable Suspended Particulates (PM10)
# 9. SO2 = Sulphur Dioxide
# --------------------------------------------------
# ## All 18 stations are as follows:
# ### General Stations
# 1. Western
# 2. Southern
# 3. Eastern
# 4. KwunTong
# 5. ShamShuiPo
# 6. KwaiChung
# 7. TsuenWan
# 8. TseungKwanO
# 9. YuenLong
# 10. TuenMun
# 11. TungChung
# 12. TaiPo
# 13. ShaTin
# 14. North
# 15. TapMun
# ### Roadside Stations
# 16. CausewayBay
# 17. Central
# 18. MongKok
# --------------------------------------------------
