import sys
from pyspark import SparkConf, SparkContext
from csv import reader
conf = SparkConf().setAppName("task1")
sc = SparkContext(conf=conf)

#Loading Parking Violations
myline1 = sc.textFile(sys.argv[1], 1)
myline1 = myline1.mapPartitions(lambda x: reader(x))

#Loading Open Violations
myline2 = sc.textFile(sys.argv[2], 1)
myline2 = myline2.mapPartitions(lambda x: reader(x))

alltheviolations = myline1.map(lambda x: (x[0],(x[14],x[6],x[2],x[1])))
alltheopenviolations = myline2.map(lambda x: (x[0],1))


finaloutput = alltheviolations.subtractByKey(alltheopenviolations)

finaloutput = finaloutput.sortByKey().map(lambda x: (x[0],x[1][0],x[1][1],x[1][2], x[1][3]))
finaloutput.map(lambda (a,b,c,d,e): "{0}\t{1}, {2}, {3}, {4}".format(a,b,c,d,e)).saveAsTextFile("task1.out")
