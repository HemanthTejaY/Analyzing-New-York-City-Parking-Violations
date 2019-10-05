import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string,date_format
from csv import reader
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import Row
spark = SparkSession.builder.getOrCreate()

spark = SparkSession.builder.getOrCreate()


myopenv = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
myopenv.createOrReplaceTempView("myopenv")


#For every given license type , we find the average and also the total amounts that are due

out = spark.sql("SELECT license_type, SUM(amount_due) AS sum_amount, AVG(amount_due) AS avg_amount FROM myopenv GROUP BY license_type")
out.select(format_string('%s\t%.2f, %.2f',out.license_type,out.sum_amount,out.avg_amount)).write.save("task3-sql.out",format="text")


