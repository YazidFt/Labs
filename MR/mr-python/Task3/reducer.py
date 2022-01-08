#!/usr/bin/python


import sys

tot_nb = 0
tot_cost = 0.0


for line in sys.stdin:

    nb, cost = line.strip().split('\t')
    nb = int(nb)
    cost = float(cost)

    tot_nb += nb
    tot_cost += cost


print("total number of sales is: {0} and the total value of sales is: {1}".format(tot_nb, tot_cost))
    
    
