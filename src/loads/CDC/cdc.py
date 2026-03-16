from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("CDC_Example") \
    .getOrCreate()

cdc_data = [
    (1, "Aishwarya", "Pune", "UPDATE"),
    (2, "Sneha", "Delhi", "INSERT"),
    (3, None, None, "DELETE")
]

columns = ["id", "name", "city", "operation"]

cdc_df = spark.createDataFrame(cdc_data, columns)

print("CDC Data")
cdc_df.show()

insert_df = cdc_df.filter(col("operation") == "INSERT")

update_df = cdc_df.filter(col("operation") == "UPDATE")

delete_df = cdc_df.filter(col("operation") == "DELETE")

print("Insert Records")
insert_df.show()

print("Update Records")
update_df.show()

print("Delete Records")
delete_df.show()