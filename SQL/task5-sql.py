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


out = spark.sql("SELECT plate_id as thepid, registration_state as themid, COUNT(*) AS max_count FROM myparking GROUP BY plate_id, registration_state ORDER BY max_count DESC LIMIT 1")

out.select(format_string('%s, %s\t%d',out.thepid,out.themid,out.max_count)).write.save("task5-sql.out",format="text")
