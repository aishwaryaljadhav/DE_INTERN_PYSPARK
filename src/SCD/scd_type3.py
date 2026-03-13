from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName("SCD_Type3").getOrCreate()

data = [
(101,"Aisha","Bangalore",' ')
]

df = spark.createDataFrame(data,
["id","name","current_city","previous_city"])

scd3_df = df.withColumn("previous_city",df.current_city) \
            .withColumn("current_city",lit("Mumbai"))

scd3_df.show()