#!/usr/bin/env python

import sys
import string

import os

mycurrentkeyval = None
curramt = 0
finalcount = 0

for line in sys.stdin:

        line = line.strip()

        key, amount = line.split('\t',1)
        try:
            	amount = float(amount)
        except ValueError:
                continue

        if mycurrentkeyval == key:
                curramt += amount
                finalcount += 1
        else:

             	if mycurrentkeyval:
                        print ('{0}\t{1:.2f}, {2:.2f}'.format(mycurrentkeyval,round(curramt,2),round(curramt/finalcount,2)))

                mycurrentkeyval = key
                curramt = amount
                finalcount = 1
if mycurrentkeyval == key:
        print ('{0}\t{1:.2f}, {2:.2f}'.format(mycurrentkeyval,round(curramt,2),round(curramt/finalcount,2)))

