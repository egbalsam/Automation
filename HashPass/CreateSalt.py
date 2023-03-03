import os
import sys
import numpy as np
import hashlib
from datetime import datetime

hashseed = os.getenv('PYTHONHASHSEED')
if not hashseed:
    os.environ['PYTHONHASHSEED'] = '0'
    os.execv(sys.executable, [sys.executable] + sys.argv)

def gettime():
    now = datetime.now()
    return now.strftime("%H:%M:%S")
    
def createsalt(pw):
# def createsalt():
    i = 0 #rand seed iteration
    x = 5 #hash difficulty
    y = '0'*x
    z = x*-1

    starttime = gettime()

    while i > -1:
        np.random.seed(i)
        x = np.random.randint(0,2147483648)
        # p = str(hex(hash('Password!123' + str(x))))
        p = str(hex(hash(pw + str(x))))
        if p[z:] == y:
            k = i
            i = -1
            endtime = gettime()
        else:
            i+=1
    # print(p)
    # print(p[z:])
    # print(x)
    # print('Start at: ' + str(starttime))
    # print('End at: ' + str(endtime))

    return k,x

def hashme(var):
    p = str(hex(hash(var)))
    return p