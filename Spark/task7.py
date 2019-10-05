import sys
from pyspark import SparkConf, SparkContext
from decimal import Decimal
from csv import reader
conf = SparkConf().setAppName("task7")
sc = SparkContext(conf=conf)

#Code that helps us process the dataset parking-violations.csv
myline1 = sc.textFile(sys.argv[1], 1)
myline1 = myline1.mapPartitions(lambda x: reader(x))

#We gather the average number of violations for weekdays and weekends for a given violation code

weekdayviolated = myline1.map(lambda x: (x[2],(0 if int(x[1][-2:]) in (5,6,12,13,19,20,26,27) else 1))).reduceByKey(lambda x, y: x + y).map(lambda x: (x[0], Decimal(Decimal(x[1])/23).quantize(Decimal('.01'))))

weekendviolated = myline1.map(lambda x: (x[2],(1 if int(x[1][-2:]) in (5,6,12,13,19,20,26,27) else 0))).reduceByKey(lambda x, y: x + y).map(lambda x: (x[0], Decimal(Decimal(x[1])/8).quantize(Decimal('.01'))))

weekendviolated.fullOuterJoin(weekdayviolated).map(lambda x: (x[0],x[1][0],x[1][1])).map(lambda (k, v, l): "{0}\t{1}, {2}".format(k, v, l)).saveAsTextFile("task7.out")
