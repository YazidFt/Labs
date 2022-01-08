#!/usr/bin/python

import sys

current_category = None
tot_cost = 0

for line in sys.stdin:

    category, cost = line.strip().split("\t")
    cost = float(cost)
    
    if(current_category == category):
        tot_cost += cost
    else:
        if(current_category != None):
            print("{0}\t{1}".format(current_category, tot_cost))
        
        current_category = category
        tot_cost = cost

if(current_category != None):
    print("{0}\t{1}".format(current_category, tot_cost))



    