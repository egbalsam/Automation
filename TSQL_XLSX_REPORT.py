import csv
import pyodbc
import datetime
import time
import win32com.client
from win32com.client import Dispatch, constants


date = datetime.date.today()
year = str(date.year)
month = str(date.month)
day = str(date.day)
timestamp = time.strftime('%H%M%S')

while len(month) < 2:
	month = '0' + month
while len(day) < 2:
	day = '0' + day

#1/11: insert file location (make sure to have 2 backslashes after the drive name (i.e. c:\\) and at the end "\\")  <-----Make sure folder exists, if not, create it.
filelocation = str('C:\\FOLDER\DROP\LOCATION\Output\\')

#2/11: change name of file
filename = str(year + month + day + '_' + timestamp + '_REPORT_NAME.csv')
file = filelocation + filename

#3/11: Change server and database parameters
conn = pyodbc.connect(DRIVER='{SQL Server}',SERVER='SERVER_NAME',DATABASE='DATABSE_NAME',UID='UID',PWD='PASSWORD')
crsr = conn.cursor()
# test data
sql = """
	[ENTER SQL SCRIPT HERE]
"""
rows = crsr.execute(sql)
rowct = crsr.fetchone()
try:
	print(rowct[0])
	rows = crsr.execute(sql)
	with open(file, 'w', newline='') as csvfile:
		writer = csv.writer(csvfile)
		writer.writerow([x[0] for x in crsr.description])  # column headers
		for row in rows:
			writer.writerow(row)
			print(row)
	

	sql = """
	[ENTER ANOTHER SQL SCRIPT HERE]
	"""
	crsr.execute(sql)
	conn.commit()	

	subject = 'PROD - REPORT NAME ' + year + '/' + month + '/' + day

	print('Sending file: ' + subject)

	const=win32com.client.constants
	olMailItem = 0x0
	obj = win32com.client.Dispatch("Outlook.Application")
	newMail = obj.CreateItem(olMailItem)
	newMail.Subject = subject
	newMail.BodyFormat = 2
	newMail.HTMLBody = """<html><body><span style="font-family: \'Calibri Light\', sans-serif;"><p>PROD - REPORT NAME has been processed in PROD.  Please see attached list of updated REPORT.</p></span></body></html>"""
	newMail.To = "working email address" #<-----disable for production
	#newMail.To = "working email address" #<-----enable for production
	attachment1 = file
	newMail.Attachments.Add(Source=attachment1)
	newMail.send
	print(subject + ' successfully sent')
	
except Exception as e:
	print('No records found')
	subject = 'PROD - NO CHANGES REPORT OUTPUT ' + year + '/' + month + '/' + day
	const=win32com.client.constants
	olMailItem = 0x0
	obj = win32com.client.Dispatch("Outlook.Application")
	newMail = obj.CreateItem(olMailItem)
	newMail.Subject = subject
	newMail.BodyFormat = 2
	newMail.HTMLBody = """<html><body><span style="font-family: \'Calibri Light\', sans-serif;"><p>PROD - Daily REPORT has been processed.  NO NEW RECORDS CHANGED.</p><p>""" + str(e) + """</p></span></body></html>"""
	newMail.To = "working email address" #<-----disable for production
	#newMail.To = "working email address" #<-----enable for production
	newMail.send
	print(subject + ' successfully sent')
	

