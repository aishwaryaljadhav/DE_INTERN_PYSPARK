from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("StringProgram2").getOrCreate()

data = [
(1,"  john doe  ","john123@gmail.com","electronics-phone","  new york  ","EMP-1001"),
(2," alice smith ","alice456@yahoo.com","fashion-dress","  los angeles","EMP-1002"),
(3,"bob brown  ","bob789@hotmail.com","home-furniture","chicago   ","EMP-1003"),
(4,"  clara johnson","clara111@gmail.com","electronics-laptop","  houston  ","EMP-1004"),
(5,"david lee ","david222@yahoo.com","sports-shoes","phoenix   ","EMP-1005"),
(6,"  emily davis ","emily333@gmail.com","beauty-makeup","  philadelphia","EMP-1006")
]

columns = ["id","name","email","product","city","employee_code"]

df = spark.createDataFrame(data,columns)
