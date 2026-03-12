from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("UDFExample").getOrCreate()

data = [
(1,"Amit",85),
(2,"Riya",72),
(3,"John",55),
(4,"Sara",90)
]

columns = ["ID","Name","Marks"]

df = spark.createDataFrame(data, columns)

def grade(mark):
    if mark > 80:
        return "A"
    elif mark > 70:
        return "B"
    else:
        return "C"

grade_udf = udf(grade, StringType())

df2 = df.withColumn("Grade", grade_udf(col("Marks")))

df2.show()