#SOURCE: https://medium.com/geekculture/web-scraping-tables-in-python-using-beautiful-soup-8bbc31c5803e

# Importing the required libraries
import requests
import pandas as pd
from bs4 import BeautifulSoup
import pyodbc
from datetime import datetime

# Downloading contents of the web page
url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
data = requests.get(url).text


# Creating BeautifulSoup object
soup = BeautifulSoup(data, 'html.parser')

# Verifying tables and their classes
# print('Classes of each table:')
# for table in soup.find_all('table'):
    # print(table.get('class'))


# Creating list with all tables
tables = soup.find_all('table')

#  Looking for the table with the classes 'wikitable' and 'sortable'
table = soup.find('table', class_='wikitable sortable')

# Defining of the dataframe
df = pd.DataFrame(columns=['Symbol', 'Security', 'SEC Filings', 'GICS Sector', 'GICS Sub Industry', 'HQ Location','Date First Added','CIK','Founded','ID'])

conn = pyodbc.connect(DRIVER='{SQL Server}',SERVER='LAPTOP-D6TKOBQR\SQLEXPRESS01',DATABASE='stock',Trusted_connection='yes')
crsr = conn.cursor()

# Collecting Ddata
for row in table.tbody.find_all('tr'):    
    # Find all data for each column
	columns = row.find_all('td')
	
	if(columns != []):
		symbol = columns[0].text.strip()
		security = columns[1].text.strip()
		security = security.replace("""'""", '' )
		filings = columns[2].text.strip()
		sector = columns[3].text.strip()
		industry = columns[4].text.strip()
		location = columns[5].text.strip()
		added = """'"""+columns[6].text.strip()+"""'"""
		cik = columns[7].text.strip()
		founded = columns[8].text.strip()
		id = columns[0].text.strip() + '-' + columns[6].text.strip()
		
		if added == '':
			added = '1900-01-01'
		elif len(added) > 12:
			added = added[0:11]+"""'"""
			print(added)
		
		# print("""INSERT INTO dbo.IndexStocks (Symbol,Security,SEC_Filings,GICS_Sector,GICS_Sub_Industry,HQ_Location,Date_First_Added,CIK,Founded,ID) VALUES ('"""+symbol+"""','"""+security+"""','"""+filings+"""','"""+sector+"""','"""+industry+"""','"""+location+"""','"""+added+"""','"""+cik+"""','"""+founded+"""','"""+id+"""')""")
		try:
			crsr.execute("""INSERT INTO dbo.IndexStocks (Symbol,Security,SEC_Filings,GICS_Sector,GICS_Sub_Industry,HQ_Location,Date_First_Added,CIK,Founded,ID) VALUES ('"""+symbol+"""','"""+security+"""','"""+filings+"""','"""+sector+"""','"""+industry+"""','"""+location+"""',"""+added+""",'"""+cik+"""','"""+founded+"""','"""+id+"""')""")
			conn.commit()
		except Exception as e:
			print("""INSERT INTO dbo.IndexStocks (Symbol,Security,SEC_Filings,GICS_Sector,GICS_Sub_Industry,HQ_Location,Date_First_Added,CIK,Founded,ID) VALUES ('"""+symbol+"""','"""+security+"""','"""+filings+"""','"""+sector+"""','"""+industry+"""','"""+location+"""',"""+added+""",'"""+cik+"""','"""+founded+"""','"""+id+"""')""")
			print(str(e))
			

# df = df.append({'Symbol': symbol,  'Security': security, 'SEC Filings': filings, 'GICS Sector': sector, 'GICS Sub Industry': industry, 'HQ Location': location, 'Date First Added': added,'CIK': cik,'Founded': founded,'ID':id}, ignore_index=True)