#SOURCE: https://medium.com/geekculture/web-scraping-tables-in-python-using-beautiful-soup-8bbc31c5803e

# Importing the required libraries
import requests
import pandas as pd
from bs4 import BeautifulSoup

# Downloading contents of the web page
url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
data = requests.get(url).text


# Creating BeautifulSoup object
soup = BeautifulSoup(data, 'html.parser')

# Verifying tables and their classes
print('Classes of each table:')
for table in soup.find_all('table'):
    print(table.get('class'))


# Creating list with all tables
tables = soup.find_all('table')

#  Looking for the table with the classes 'wikitable' and 'sortable'
table = soup.find('table', class_='wikitable sortable')

# Defining of the dataframe
df = pd.DataFrame(columns=['Symbol', 'Security', 'SEC Filings', 'GICS Sector', 'GICS Sub Industry', 'HQ Location','Date First Added'])

# Collecting Ddata
for row in table.tbody.find_all('tr'):    
    # Find all data for each column
	columns = row.find_all('td')
	
	if(columns != []):
		symbol = columns[0].text.strip()
		security = columns[1].text.strip()
		filings = columns[2].text.strip()
		sector = columns[3].text.strip()
		industry = columns[4].text.strip()
		location = columns[5].text.strip()
		added = columns[6].text.strip()
		
		df = df.append({'Symbol': symbol,  'Security': security, 'SEC Filings': filings, 'GICS Sector': sector, 'GICS Sub Industry': industry, 'HQ Location': location, 'Date First Added': added}, ignore_index=True)

print(df['Symbol'].to_string(index=False))