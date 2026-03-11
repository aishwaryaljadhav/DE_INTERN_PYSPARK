from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("MathDataset").getOrCreate()

data = [
(1,"Laptop","70000","2025-03-01"),
(2,"Mobile","25000","2025-03-05"),
(3,"Shoes","3000","2025-03-08")
]

columns = ["ID","Product","Price","Order_Date"]

df = spark.createDataFrame(data,columns)