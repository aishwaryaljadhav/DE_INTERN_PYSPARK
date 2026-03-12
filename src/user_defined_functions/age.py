from pyspark.sql import SparkSession
from pyspark.sql.functions import*
from pyspark.sql.types import*

spark = SparkSession.builder.appName("UDFExample").getOrCreate()

data = [
(1,"Rahul",15),
(2,"Priya",28),
(3,"Mahesh",65),
(4,"Anita",45)
]

columns = ["ID","Name","Age"]

df = spark.createDataFrame(data, columns)
def age_group(age):
    if age < 18:
        return "Minor"
    elif age <= 60:
        return "Adult"
    else:
        return "Senior"

age_udf = udf(age_group, StringType())

df.withColumn("Age_Group", age_udf("Age")).show()