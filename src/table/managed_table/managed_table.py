from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ManagedTableExample") \
    .enableHiveSupport() \
    .getOrCreate()

data = [
    (1, "Aishwarya", "Bangalore"),
    (2, "Rahul", "Mumbai"),
    (3, "Sneha", "Pune")
]

columns = ["id", "name", "city"]

df = spark.createDataFrame(data, columns)

df.write.saveAsTable("employees_managed")

spark.sql("SELECT * FROM employees_managed").show()

spark.sql("SHOW TABLES").show()

spark.sql("DROP TABLE employees_managed")