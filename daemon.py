from globals import *
import time

LIST = []

def daemon(f):
    LIST.append(f)
    return f

#@daemon
def gather():
    while True:
        print('dummy gatherer daemon wakeup')
        time.sleep(DAEMONS_INTERVAL)
