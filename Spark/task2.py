import sys
from pyspark import SparkConf, SparkContext
from csv import reader
conf = SparkConf().setAppName("task2")
sc = SparkContext(conf=conf)

#Code that helps us process the dataset parking-violations.csv
myline1 = sc.textFile(sys.argv[1], 1)
myline1 = myline1.mapPartitions(lambda x: reader(x))

#We compute the frequency of the violation code 
myviolationcodes = myline1.map(lambda x: (x[2],1)).reduceByKey(lambda x, y: x + y)
myviolationcodes.map(lambda (k, v): "{0}\t{1}".format(k, v)).saveAsTextFile("task2.out")
