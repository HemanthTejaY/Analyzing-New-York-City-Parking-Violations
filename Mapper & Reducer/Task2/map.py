#!/usr/bin/env python
#Using this code, we attain the key and add a value '1' next to it. We thereby attain the violation codes for every line

import sys

for line in sys.stdin:
        line = line.strip()
        xyz = line.split(',')
        print("{0}\t{1}".format(xyz[2], '1'))

