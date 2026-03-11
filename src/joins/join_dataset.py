from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("JoinData").getOrCreate()

customers = [
(1,"Aishwarya","Mumbai"),
(2,"Rahul","Delhi"),
(3,"Sneha","Pune"),
(4,"Karan","Bangalore")
]

orders = [
(1,"Laptop",60000),
(2,"Phone",20000),
(2,"Headphones",3000),
(5,"Tablet",15000)
]

df_customers = spark.createDataFrame(customers,["Customer_ID","Name","City"])
df_orders = spark.createDataFrame(orders,["Customer_ID","Product","Price"])