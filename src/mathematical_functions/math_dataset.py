from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("MathDataset").getOrCreate()

data = [
(101,"Laptop",70000,2.5,-10),
(102,"Phone",25000,1.8,-5),
(103,"Tablet",15000,1.2,-3),
(104,"Monitor",12000,1.6,-7),
(105,"Keyboard",3000,0.8,-2),
(106,"Mouse",1500,0.5,-1),
(107,"Headphones",5000,1.3,-4),
(108,"Printer",18000,2.1,-6)
]

columns = ["Product_ID","Product","Price","Weight","Discount"]

df = spark.createDataFrame(data,columns)

df.show()