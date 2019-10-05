import sys
from decimal import Decimal
from pyspark import SparkConf, SparkContext
from csv import reader
conf = SparkConf().setAppName("task3")

sc = SparkContext(conf=conf)

myline1 = sc.textFile(sys.argv[1], 1)
myline1 = myline1.mapPartitions(lambda x: reader(x))
amount = myline1.map(lambda x: (x[2],Decimal(x[12])))
sum = amount.reduceByKey(lambda x, y: x + y)

#For every given license type , we find the average and also the total amounts that are due

thelicensetype = myline1.map(lambda x: (x[2], 1))
thelicensecount =  thelicensetype.reduceByKey(lambda x, y: x + y)
thesumplusavg=sum.join(thelicensecount)


thesumplusavg=thesumplusavg.map(lambda x: (x[0], x[1][0], Decimal((x[1][0]/x[1][1]).quantize(Decimal('.01')))))
thesumplusavg.map(lambda (k, v, l): "{0}\t{1}, {2}".format(k, v, l)).saveAsTextFile("task3.out")
