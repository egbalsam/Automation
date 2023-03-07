import os
import sys
import numpy as np
import hashlib

hashseed = os.getenv('PYTHONHASHSEED')
if not hashseed:
    os.environ['PYTHONHASHSEED'] = '0'
    os.execv(sys.executable, [sys.executable] + sys.argv)


spc = [33,35,36,38,42,64,45,47,92]
num = [50,51,52,53,54,55,56,57]
upp = [65,66,67,68,69,70,71,72,74,75,76,77,78,80,81,82,83,84,85,86,87,88,89,90]
lwr = [97,98,99,100,101,102,103,104,105,106,107,109,110,112,113,114,115,116,117,118,119,120,121,122]

# print(len(spc)) #9, 0-8
# print(len(num)) #8, 0-7
# print(len(upp)) #24, 0-23
# print(len(lwr)) #24, 0-23

def passgen(seed):
    np.random.seed(seed)
    i=1
    pas = []
    while i < 5:
        x = np.random.randint(0,len(spc)-1)
        pas.append(spc[x])
        x = np.random.randint(0,len(num)-1)
        pas.append(num[x])
        x = np.random.randint(0,len(upp)-1)
        pas.append(upp[x])
        x = np.random.randint(0,len(lwr)-1)
        pas.append(lwr[x])        
        i+=1
    np.random.shuffle(pas)
    return pas

