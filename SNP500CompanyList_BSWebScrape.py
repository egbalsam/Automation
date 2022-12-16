#Source: https://medium.com/analytics-vidhya/web-scraping-wiki-tables-using-beautifulsoup-and-python-6b9ea26d8722

# ‘Data is the new oil’
# As an aspiring data scientist, I do a lot of projects which involve scraping data from various websites. Some companies like Twitter do provide APIs to get their information in a more organized way while we have to scrape other websites to get data in a structured format.

# The general idea behind web scraping is to retrieve data that exists on a website and convert it into a format that is usable for analysis. In this tutorial, I will be going through a detail but simple explanation of how to scrape data in Python using BeautifulSoup. I will be scraping Wikipedia to find out all the countries in Asia.

import requests
from bs4 import BeautifulSoup
import pandas as pd

# Firstly we are going to import requests library. Requests allows you to send organic, grass-fed HTTP/1.1 requests, without the need for manual labor.

# import requests

# Now we assign the link of the website through which we are going to scrape the data and assign it to variable named website_url.

# requests.get(url).text will ping a website and return you HTML of the website.

website_url = requests.get('https://en.wikipedia.org/wiki/List_of_Asian_countries_by_area').text

soup = BeautifulSoup(website_url,'lxml')

# If you carefully inspect the HTML script all the table contents i.e. names of the countries which we intend to extract is under class Wikitable Sortable.

# So our first task is to find class ‘wikitable sortable’ in the HTML script.

# My_table = soup.find(‘table’,{‘class’:’wikitable sortable’})

# Under table class ‘wikitable sortable’ we have links with country name as title.

#print(soup.prettify())

# Now to extract all the links within <a>, we will use find_all().

My_table = soup.find('table',{'class':'wikitable sortable'})

links = My_table.findAll('a')

# From the links, we have to extract the title which is the name of countries.

# To do that we create a list Countries so that we can extract the name of countries from the link and append it to the list countries.

Countries = []
for link in links:
	Countries.append(link.get('title'))
	
# Convert the list countries into Pandas DataFrame to work in python.

df = pd.DataFrame()
df['Country'] = Countries

print(df)

# Thank you for reading my first article on Medium. I will make it a point to write regularly about my journey towards Data Science. Thanks again for choosing to spend your time here — means the world.

# You can find my code on Github.

# https://github.com/stewync/Web-Scraping-Wiki-tables-using-BeautifulSoup-and-Python/blob/master/Scraping%2BWiki%2Btable%2Busing%2BPython%2Band%2BBeautifulSoup.ipynb