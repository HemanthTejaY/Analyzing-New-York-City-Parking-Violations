import sys
from pyspark import SparkConf, SparkContext
from csv import reader
conf = SparkConf().setAppName("task5")
sc = SparkContext(conf=conf)

myline1 = sc.textFile(sys.argv[1], 1)
myline1 = myline1.mapPartitions(lambda x: reader(x))

#We compute the vehicle that has had the greatest number of violations 
#We identify a vehicle using its PlateID and the state of registration

id = myline1.map(lambda x: ((x[14],x[16]),1)).reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[1], False)
thetopmostviolator = sc.parallelize(id.take(1)).map(lambda x: (x[0][0], x[0][1], x[1]))
thetopmostviolator.map(lambda (k, v, l): "{0}, {1}\t{2}".format(k, v, l)).saveAsTextFile("task5.out")
