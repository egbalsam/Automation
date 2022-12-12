import cx_Oracle
import pandas as pd
import xlwt
import datetime
import dateutil
import os
import win32com.client
from win32com.client import Dispatch, constants
import shutil


environment = 'PROD'


try:
    if environment == 'PROD':
        print('The environment is PROD')
    else:
        print('The environment is TEST')
except:
    environment = input('Please define the environment: ')
    if environment == 'PROD':
        print('The environment is PROD')
    else:
        print('The environment is TEST')



#To run ADHOC, change todayDate to month+1 for report (ex: 5/12/2021 will output for 4/1/2021, 4/30/2021 and 02/01/2021 variables (begdate,enddate,twomosprior))
todayDate = datetime.date.today()
firstofmonth = todayDate.replace(day=1)
begdate = firstofmonth-dateutil.relativedelta.relativedelta(months=1)
enddate = begdate+dateutil.relativedelta.relativedelta(months=1)-dateutil.relativedelta.relativedelta(days=1)
twomosprior = begdate-dateutil.relativedelta.relativedelta(months=2)

print('Report for period starting: ' + str(begdate))
print('Report for period ending: ' + str(enddate))

begdate=begdate.strftime('%m/%d/%Y')
enddate=enddate.strftime('%m/%d/%Y')
twomosprior=twomosprior.strftime('%m/%d/%Y')

date = todayDate-dateutil.relativedelta.relativedelta(months=1)
year = str(date.year)
month = str(date.month)
day = str(date.day)

while len(month) < 2:
    month = '0' + month
while len(day) < 2:
    day = '0' + day

reportname = str('NRZ_MasterServicing_SLA_Trigger_Report_F_' + year + '_' + month)

filename = str(reportname + '.xlsx')

origlocation = r'\\FILE\DIRECTORY\LOCATION'+ '\\' +str(year) + str(month) + '\\'
filelocation = r'\\FILE\DIRECTORY\LOCATION'+ '\\' +str(year) + '\\'

if not os.path.exists(filelocation):
    os.makedirs(filelocation)

origfile = origlocation + filename
file = filelocation + filename

try:
    shutil.copy2(origfile, file)
    print('File was successfully moved from:')
    print('\t' + origlocation)
    print('to:')
    print('\t' + filelocation)
except Exception as e:
    print('File FAILED to be moved for the following exception:')
    print('\t' + str(e))

print(file)

subject = reportname

print(subject)

try:
    
    # Email source: https://gist.github.com/ITSecMedia/b45d21224c4ea16bf4a72e2a03f741af
    const=win32com.client.constants
    olMailItem = 0x0
    obj = win32com.client.Dispatch("Outlook.Application")
    newMail = obj.CreateItem(olMailItem)
    newMail.Subject = subject
    newMail.BodyFormat = 2 # olFormatHTML https://msdn.microsoft.com/en-us/library/office/aa219371(v=office.11).aspx
    newMail.HTMLBody = """<p>Please see attached """+reportname+""". Let me know if you have any questions.</p>
    <p>Thanks,</p>
    <p><strong>SIGNATURE</strong>
    <br />JOB TITLE<span style="font-size: 14px; color: gray; font-family: 'Calibri Light', sans-serif;"> | </span>DEPARTMENT</p>
    <p><span style="font-size: 14px; color: gray; font-family: 'Calibri Light', sans-serif;">
    ADDRESS
    <br />PHONE NUMBER<br /><a href="EMAIL ADDRESS">EMAIL ADDRESS</a></span></p>"""
    if environment == 'TEST':
        newMail.To = 'EMAIL ADDRESS' #<-----disable for production
    else:
        newMail.To = 'EMAIL ADDRESS' #<-----enable for production
        newMail.CC = 'EMAIL ADDRESS ' #<-----enable for production
        newMail.BCC = 'EMAIL ADDRESS'
    attachment1 = file
    newMail.Attachments.Add(Source=attachment1)
    newMail.send
    print(subject + ' successfully sent')
except Exception as e:
    if environment == 'TEST':
        subject = 'FAILED - ' + reportnum + ' - '+ subject
    else:
        subject = 'FAILED - ' + reportnum + ' - '+ subject

    const=win32com.client.constants
    olMailItem = 0x0
    obj = win32com.client.Dispatch("Outlook.Application")
    newMail = obj.CreateItem(olMailItem)
    newMail.Subject = subject
    newMail.BodyFormat = 2 # olFormatHTML https://msdn.microsoft.com/en-us/library/office/aa219371(v=office.11).aspx
    newMail.HTMLBody = """<html><body><span style="font-family: 'Calibri Light', sans-serif;">
    <p>The """+reportname+""" failed to run properly.  See error below:</p><p>""" + str(e) + """</p></span></body></html>"""
    newMail.To = 'EMAIL ADDRESS'
    newMail.send