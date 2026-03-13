from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce

spark = SparkSession.builder.appName("SCD_Type1").getOrCreate()

old_data = [
(101,"Aisha","Bangalore"),
(102,"Rahul","Delhi")
]

old_df = spark.createDataFrame(old_data,["id","name","city"])

new_data = [
(101,"Aisha","Mumbai")
]

new_df = spark.createDataFrame(new_data,["id","name","city"])

scd1_df = old_df.alias("old").join(
    new_df.alias("new"),
    "id",
    "left"
).select(
    "id",
    coalesce("new.name","old.name").alias("name"),
    coalesce("new.city","old.city").alias("city")
)

scd1_df.show()