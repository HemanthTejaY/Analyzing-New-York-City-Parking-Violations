#!/usr/bin/env python

import sys
import string
import csv
import os

reader=csv.reader(sys.stdin,delimiter=',')

for myent in reader:
        thekey = myent[2]
        try:
            	value = float(myent[12])
        except ValueError:
                continue

        print ('{0:s}\t{1:f}'.format(thekey,value))

