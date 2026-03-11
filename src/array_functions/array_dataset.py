from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ArrayDataset").getOrCreate()

data = [
(101,"Aishwarya","Data","Python","SQL","Spark"),
(102,"Rahul","Backend","Java","Spring","SQL"),
(103,"Neha","Data","Python","Hadoop","Hive"),
(104,"Arjun","DevOps","Docker","Kubernetes","Linux"),
(105,"Sneha","Data","Python","SQL","Airflow")
]

cols = ["Emp_ID","Name","Department","Skill1","Skill2","Skill3"]

df = spark.createDataFrame(data,cols)