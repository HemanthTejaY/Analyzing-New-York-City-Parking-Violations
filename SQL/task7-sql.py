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

#We gather the average number of violations for weekdays and weekends for a given violation code

out = spark.sql("SELECT t1.violation_code, t1.Weekend, t2.Weekdays FROM (SELECT violation_code, COUNT(violation_code)/8 AS Weekend FROM myparking WHERE (EXTRACT(DAY FROM issue_date)=27 OR EXTRACT(DAY FROM issue_date)=26 OR EXTRACT(DAY FROM issue_date)=20 OR EXTRACT(DAY FROM issue_date)=19 OR EXTRACT(DAY FROM issue_date)=13 OR EXTRACT(DAY FROM issue_date)=12 OR EXTRACT(DAY FROM issue_date)=6 OR EXTRACT(DAY FROM issue_date)=5) GROUP BY violation_code) t1  INNER JOIN (SELECT violation_code, COUNT(violation_code)/23 AS Weekdays FROM myparking WHERE NOT (EXTRACT(DAY FROM issue_date)=27 OR EXTRACT(DAY FROM issue_date)=26 OR EXTRACT(DAY FROM issue_date)=20 OR EXTRACT(DAY FROM issue_date)=19 OR EXTRACT(DAY FROM issue_date)=13 OR EXTRACT(DAY FROM issue_date)=12 OR EXTRACT(DAY FROM issue_date)=6 OR EXTRACT(DAY FROM issue_date)=5) GROUP BY violation_code) t2 ON t1.violation_code = t2.violation_code")

out.select(format_string('%d\t%.2f, %.2f',t1.violation_code,ROUND(t1.Weekend,2),ROUND(t2.Weekdays,2))).write.save("task7-sql.out",format="text")






