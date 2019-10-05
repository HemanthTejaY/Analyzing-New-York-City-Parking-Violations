#!/usr/bin/env python

import sys
import csv

#Using the first if-statement we check if "line" is present in parking-data.csv
#Using the second if-statement we check if "line" is present in parking-data.csv
reader=csv.reader(sys.stdin,delimiter=',')

for computeWords in reader:

        if len(computeWords)==22:
                print ("{0}\t{1}={2}={3}={4}={5}".format(computeWords[0],computeWords[14],computeWords[6],computeWords[2],computeWords[1],'parking'))
        if len(computeWords)==18:
                print ("{0}\t{1}={2}={3}={4}={5}".format(computeWords[0],computeWords[1],computeWords[5],computeWords[7],computeWords[9],'open'))

