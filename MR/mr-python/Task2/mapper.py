#!/usr/bin/python

import sys

for line in sys.stdin:

    data = line.strip().split("\t")
    n = len(data)
    
    if(n >= 6):
        print("{0}\t{1}".format(data[n-3],data[n-2]))
    
    


    


    
    






