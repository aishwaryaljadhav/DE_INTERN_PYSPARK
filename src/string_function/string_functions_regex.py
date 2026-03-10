from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("StringProgram4").getOrCreate()

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

df.select(
    lpad("employee_code",12,"*").alias("left_padded_code"),
    rpad("employee_code",12,"*").alias("right_padded_code"),
    repeat("city",2).alias("repeated_city"),
    regexp_replace("email","[0-9]","").alias("email_without_numbers"),
    regexp_extract("email","@(.+)",1).alias("email_domain")
).show(truncate=False)



