#!/usr/bin/python


import sys


for line in sys.stdin:
    
    data = line.strip().split('\t')
    n = len(data)
    print("{0}\t{1}".format(1,data[n-2]))
    
    



