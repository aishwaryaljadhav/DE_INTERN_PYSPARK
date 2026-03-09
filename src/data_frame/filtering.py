from pyspark.sql import SparkSession
spark=SparkSession.builder\
    .appName("filtering")\
    .getOrCreate()

data = [
(1,"Aishwarya",21,"Bangalore"),
(2,"Rahul",22,"Mumbai"),
(3,"Neha",20,"Delhi"),
(4,"Amit",23,"Mumbai"),
(1,"Aishwarya",21,"Bangalore")
]

columns = ["id","name","age","city"]

df = spark.createDataFrame(data,columns)

df.show()

df.filter(df.age>21).show()

df.filter((df.age>21)&(df.city=='Bangalore')).show()

df.dropDuplicates().show()

