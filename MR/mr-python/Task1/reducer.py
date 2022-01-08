#!/usr/bin/python

import sys

current_store = None
total_cost = 0

for line in sys.stdin:
    
    store, cost = line.strip().split("\t")
    cost = float(cost)
    
    if(store == current_store):
        total_cost += cost
    else:
        if(current_store != None):
            print("{0}\t{1}".format(current_store, total_cost))
        
        current_store = store
        total_cost = cost


if(current_store != None):
    print("{0}\t{1}".format(current_store, total_cost))







    

