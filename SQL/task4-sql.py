import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string,date_format
from csv import reader
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import Row
spark = SparkSession.builder.getOrCreate()

spark = SparkSession.builder.getOrCreate()

myparking = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
myparking.createOrReplaceTempView("myparking")

#We compute the total number of violations in the state of New York and also in "other" states

out = spark.sql("SELECT 'NY' as STATE,count(registration_state) as countreg FROM myparking WHERE registration_state='NY' UNION SELECT 'OTHERS' as STATE, count(registration_state) FROM myparking WHERE NOT registration_state='NY' ORDER BY STATE")
out.select(format_string('%s\t%d',out.STATE,out.countreg)).write.save("task4-sql.out",format="text")


