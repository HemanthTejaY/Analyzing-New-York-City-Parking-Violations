import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string,date_format
from csv import reader
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import Row
spark = SparkSession.builder.getOrCreate()

myparking = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
myopenv = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[2])

myparking.createOrReplaceTempView("myparking")
myopenv.createOrReplaceTempView("myopenv")

#We carry out a join of the csv files that we process and then subtract by the varible - Summon Number

out = spark.sql("SELECT myparking.summons_number, plate_id, violation_precinct, violation_code, myparking.issue_date FROM myparking LEFT JOIN myopenv ON myparking.summons_number = myopenv.summons_number WHERE myopenv.summons_number IS NULL")

out.select(format_string('%d\t%s, %d, %d, %s',out.summons_number,out.plate_id,out.violation_precinct,out.violation_code,date_format(out.issue_date,'yyyy-MM-dd'))).write.save("task1-sql.out",format="text")



