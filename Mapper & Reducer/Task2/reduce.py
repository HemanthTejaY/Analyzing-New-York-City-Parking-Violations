#!/usr/bin/env python
import sys

maintaincount = 0
currentvalue = 0

for line in sys.stdin:
    line = line.strip()

    #We obtain the key value pairs once we remove the trailing whitespaces
    key, val = line.split('\t', 1)
    val = int(val)
    key = int(key)

    #We compute the different frequencies for the different violation types, i.e., we compute the number of violations, each violation type has
    if currentvalue == key:
        maintaincount = maintaincount + val
    else:
	if maintaincount:
            print('{0:s}\t{1:s}'.format(str(currentvalue), str(maintaincount)))
        currentvalue = key
        maintaincount = val
if maintaincount:
    print('{0:s}\t{1:s}'.format(str(currentvalue), str(maintaincount)))


