from pyspark.sql.functions import *
from sales_dataset import df

df.groupBy("Customer").agg(
    collect_list("Product").alias("Products_List")
).show(truncate=False)

df.groupBy("Customer").agg(
    collect_set("Product").alias("Unique_List")
).show(truncate=False)

df.groupBy("Category").agg(
    count_distinct("Product").alias("Distinct_Products")
).show(truncate=False)

df.groupBy("Category").agg(
    first("Product").alias("First_Product")
).show(truncate=False)

df.groupBy("Category").agg(
    last("Product").alias("Last_Product")
).show(truncate=False)