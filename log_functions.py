'''
Created on 10 mars 2014

@author: david
'''
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import atexit
from time import clock

def secondsToStr(t):
    return "%d:%02d:%02d.%03d" % \
        reduce(lambda ll,b : divmod(ll[0],b) + ll[1:],
            [(t*1000,),1000,60,60])

line = "="*40
def log(s, elapsed=None):
    print line
    print secondsToStr(clock()), '-', s
    if elapsed:
        print "Elapsed time:", elapsed
    print line
    print

def endlog():
    end = clock()
    elapsed = end-start_program
    log("End Program", secondsToStr(elapsed))

def now():
    return secondsToStr(clock())

start_program = clock()
atexit.register(endlog)
log("Start Program")

