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

#We compute the top 20 vehicles in terms of the total violations -- we the PlateID and registration state to uniquely identify a vehicle

out = spark.sql("SELECT plate_id, registration_state, COUNT(*) AS max_count FROM myparking GROUP BY plate_id, registration_state ORDER BY max_count DESC, plate_id ASC LIMIT 20")

out.select(format_string('%s, %s\t%d',out.plate_id,out.registration_state,out.max_count)).write.save("task6-sql.out",format="text")
