import os
import sys
import numpy as np
import hashlib
import pyodbc
from datetime import datetime
from CreateSalt import createsalt
from CreateSalt import hashme
from passgen import passgen

conn = pyodbc.connect(DRIVER='{SQL Server}',SERVER='OPTIPLEX\SQLEXPRESS01',DATABASE='stock',Trusted_connection='yes')
crsr = conn.cursor()


site = 'Gmail'
meta = []
tid = hex(1)
uid = ['[uid1]','[uid2]','[email1]','[email2]']
pw = 'Password!123'
var = str(tid) + ',' + pw + ','
fullpass = ''

x = createsalt(var)

print(x[0])
sd = x[0]
print(x[1])

salt = str(tid) + ',' + pw + ',' + str(x[1])
print('Salt = ' + salt)
print(hashme(salt))
for n in uid:
    userid = str(tid) + ',' + n + ',' + str(x[1])
    print('User ID = ' + userid)
    print(hashme(userid))
for n in passgen(sd):
    fullpass = fullpass + chr(n)
print('Password = ' + fullpass)