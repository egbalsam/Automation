#===============================================================
#VERSION
#===============================================================
#00.00.............................Working test of concept
#01.00.............................Changed to move mouse 5 spaces left from current position for less disruption.
#01.01.............................Adapted from mse.py for new purpose.  Trying RPA on GMAIL.
#===============================================================

import pyautogui as py
import time
import sys
#from datetime import datetime
#from playsound import playsound
import datetime
import playsound #1.2.2 version (latest version causes exception in code) to install: py -m pip install playsound==1.2.2

py.FAILSAFE = True

f = open('mselog.txt', 'w')


#===============================================================
#Use the script below to record the coordinants.  Set variables numMin to the number of different mouse locations, and leave x as 0.  This will run through the script numMin times exporting the mouse coordinants to the terminal window.  Set the sleep amount to the amount of time in seconds you want to give yourself to get the mouse cursor into position.
#===============================================================
# numMin = 4
# sleepamt = 10
# x=0

# while(x<numMin):
    # time.sleep(sleepamt) #was 300
    # now = datetime.datetime.now().time()
    # x+=1
    # postuple = py.position()
    # y=list(postuple)
    # y[0] = y[0]
    # postuple=tuple(y)
    # print('Move to coordinants: ' + str(postuple))
    # py.moveTo(postuple)
    # py.press("shift")
    # print("Movement made at {}".format(datetime.datetime.now().time()))


#===============================================================
#Once you have your coordinants from above, you are ready to set the script to run.
#===============================================================

times = 10
times = round(1998/25,0)  #enter in the total number of unread emails.  Variable will divide by 25 (which is the number of unread emails that can fit on one page) and run the loop the correct number of times.
x = 1
sleepamt = 3



while x <= times:
    print('Time: '+ str(x) + ' of ' + str(times))
    postuple = (103, 237)
    py.moveTo(postuple)
    py.click()
    time.sleep(2)
    postuple = (432, 107)
    py.moveTo(postuple)
    py.mouseDown()
    py.mouseUp()
    py.typewrite('is:unread')
    py.press('enter')
    time.sleep(2)
    postuple = (283, 210)
    py.moveTo(postuple)
    py.click()
    time.sleep(.5)
    postuple = (490, 211)
    py.moveTo(postuple)
    py.click()
    time.sleep(.5)
    x += 1