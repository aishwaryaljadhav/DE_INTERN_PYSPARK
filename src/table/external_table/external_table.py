from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ExternalTableExample") \
    .enableHiveSupport() \
    .getOrCreate()

data = [
    (101, "Laptop", 60000),
    (102, "Mobile", 20000),
    (103, "Tablet", 30000)
]

columns = ["id", "product", "price"]

df = spark.createDataFrame(data, columns)

df.write.mode("overwrite").parquet("spark-warehouse/product_data")

spark.sql("DROP TABLE IF EXISTS products_external")

spark.sql("""
CREATE TABLE products_external
USING PARQUET
LOCATION 'product_data'
""")

spark.sql("SELECT * FROM products_external").show()

spark.sql("DROP TABLE products_external")

