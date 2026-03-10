from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RetailExample").getOrCreate()

data = [
(101, "Laptop", "Electronics", 70000, "Bangalore"),
(102, "Shoes", "Fashion", 3000, "Mumbai"),
(103, "Mobile", "Electronics", 25000, "Delhi"),
(104, "Watch", "Accessories", 5000, "Pune"),
(105, "Headphones", "Electronics", 2000, "Bangalore"),
(106, "T-shirt", "Fashion", 1500, "Delhi")
]

column = ["OrderID","Product","Category","Price","City"]

df = spark.createDataFrame(data,column)

df.sort(df.Price).show()

df.sort(df.Price.desc()).show()

df.describe().show()

spark.stop()
