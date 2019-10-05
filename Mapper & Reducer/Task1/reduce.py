#!/usr/bin/env python

import sys

thecurrentkeyvalue=None
presentval=None
mypreviouskey=None
for line in sys.stdin:
        line=line.strip()
        key,value=line.split('\t',1)

        #we split the line and this helps us get rid of the extra trailing spaces

        value=value.split('=')

        #If our previous key is the same as our present key, then store it in a third variable.

        if thecurrentkeyvalue==key:
                mypreviouskey=thecurrentkeyvalue

        #However if our previous key is not the same as our present key, then we create two more cases
        #Case 1 - We check if our currentkey is equal to our previous key value and then set the value for "presentval"

        else:
             	if thecurrentkeyvalue:
                        if thecurrentkeyvalue!=mypreviouskey and presentval[-1]=='parking':
                                print ("{0}\t{1}, {2}, {3}, {4}".format(thecurrentkeyvalue,presentval[0],presentval[1],presentval[2],presentval[3]))
                thecurrentkeyvalue=key
                presentval=value

if thecurrentkeyvalue!=mypreviouskey and presentval[-1]=='parking':
        print ("{0}\t{1}, {2}, {3}, {4}".format(thecurrentkeyvalue,presentval[0],presentval[1],presentval[2],presentval[3]))
