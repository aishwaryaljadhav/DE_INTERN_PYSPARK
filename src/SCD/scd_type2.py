from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_date

spark = SparkSession.builder.appName("SCD_Type2").getOrCreate()

old_data = [
(101,"Aisha","Bangalore","2023-01-01",' ',"Y")
]

old_df = spark.createDataFrame(old_data,
["id","name","city","start_date","end_date","current_flag"])

new_data = [
(101,"Aisha","Mumbai")
]

new_df = spark.createDataFrame(new_data,
["id","name","city"])

closed_old = old_df.withColumn("end_date", current_date()) \
                   .withColumn("current_flag", lit("N"))

new_record = new_df.withColumn("start_date", current_date()) \
                   .withColumn("end_date", lit(' ')) \
                   .withColumn("current_flag", lit("Y"))

scd2_df = closed_old.union(new_record)

scd2_df.show()