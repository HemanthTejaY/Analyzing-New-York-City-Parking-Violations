import sys
from pyspark import SparkConf, SparkContext
from csv import reader
conf = SparkConf().setAppName("task4")
sc = SparkContext(conf=conf)

myline1 = sc.textFile(sys.argv[1], 1)
myline1 = myline1.mapPartitions(lambda x: reader(x))

#We compute the total number of violations in the state of New York and also in "other" states

findstate = myline1.map(lambda x: (("NY" if x[16]=="NY" else "Other" ),1)).reduceByKey(lambda x, y: x + y)
findstate.map(lambda (k, v): "{0}\t{1}".format(k, v)).saveAsTextFile("task4.out")
