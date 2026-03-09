from pyspark.sql import SparkSession
spark=SparkSession.builder\
    .appName("SQL_Demo")\
    .getOrCreate()

data = [
(1,"Aishwarya",21,"Bangalore"),
(2,"Rahul",22,"Mumbai"),
(3,"Neha",20,"Delhi"),
(4,"Amit",23,"Mumbai"),
(1,"Aishwarya",21, None)
]
column=['id','name','age','city']

df=spark.createDataFrame(data,column)

df.createTempView("students")

spark.sql('select * from students').show()


spark.sql("select id, name from students").show()

spark.sql('select * from students where age>21').show()

spark.sql('select avg(age) from students').show()

spark.sql("SELECT city, COUNT(*) FROM students GROUP BY city").show()

spark.sql('select * from students order by age desc').show()

spark.catalog.dropTempView("students")



