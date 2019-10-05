import sys
from pyspark import SparkConf, SparkContext
from csv import reader
conf = SparkConf().setAppName("task6")
sc = SparkContext(conf=conf)

myline1 = sc.textFile(sys.argv[1], 1)
myline1 = myline1.mapPartitions(lambda x: reader(x))

#We compute the top 20 vehicles in terms of the total violations -- we the PlateID and registration state to uniquely identify a vehicle

id = myline1.map(lambda x: ((x[14],x[16]),1)).reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[1], False)
thetop20mostviolators = sc.parallelize(id.take(20)).map(lambda x: (x[0][0], x[0][1], x[1]))
thetop20mostviolators.map(lambda (k, v, l): "{0}, {1}\t{2}".format(k, v, l)).saveAsTextFile("task6.out")
