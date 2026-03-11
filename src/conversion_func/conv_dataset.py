from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("ConvDataset").getOrCreate()

data = [
(101,"John","45000","4.5","2025-03-01","true","2"),
(102,"Alice","52000","3.9","2025-03-03","false","5"),
(103,"Bob","39000","4.2","2025-03-05","true","1"),
(104,"Emma","61000","4.8","2025-03-07","true","3"),
(105,"David","47000","3.7","2025-03-09","false","4")
]

columns = [
"Emp_ID",
"Name",
"Salary",
"Rating",
"Join_Date",
"Active",
"Projects"
]

df = spark.createDataFrame(data,columns)