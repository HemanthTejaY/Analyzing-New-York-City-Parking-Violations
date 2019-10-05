import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string,date_format
from csv import reader
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import Row
spark = SparkSession.builder.getOrCreate()

myparking = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
myparking.createOrReplaceTempView("myparking")

#We compute the frequency of the violation code

out = spark.sql("SELECT violation_code, COUNT(violation_code) AS value_freq FROM myparking GROUP BY violation_code")
out.select(format_string('%d\t%d',out.violation_code,out.value_freq)).write.save("task2-sql.out",format="text")

