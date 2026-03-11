from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WindowDataset").getOrCreate()

data = [
(101,"Aisha","IT",60000,2020),
(102,"Ravi","IT",70000,2019),
(103,"Arjun","IT",65000,2021),
(104,"Meena","HR",50000,2018),
(105,"John","HR",55000,2020),
(106,"Neha","HR",52000,2022),
(107,"Karan","Finance",72000,2019),
(108,"Priya","Finance",68000,2021),
(109,"Rahul","Finance",65000,2020)
]

columns = ["Emp_ID","Name","Department","Salary","Join_Year"]

df = spark.createDataFrame(data, columns)