#!/usr/bin/python

import sys

for line in sys.stdin:

    data = line.strip().split("\t")

    if(len(data) == 6):
	    print("{0}\t{1}".format(data[2],data[len(data)-2]))
    


    




