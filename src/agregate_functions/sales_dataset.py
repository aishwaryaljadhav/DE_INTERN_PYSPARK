from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName("SalesData") \
        .getOrCreate()

data = [
(1,"Alice","Laptop",60000,"Electronics"),
(2,"Bob","Phone",20000,"Electronics"),
(3,"Alice","Tablet",15000,"Electronics"),
(4,"David","Shoes",3000,"Fashion"),
(5,"Eva","Dress",4000,"Fashion"),
(6,"Bob","Laptop",55000,"Electronics"),
(7,"Alice","Shoes",2500,"Fashion"),
(8,"David","Phone",18000,"Electronics")
]

columns = ["OrderID","Customer","Product","Price","Category"]

df = spark.createDataFrame(data,columns)