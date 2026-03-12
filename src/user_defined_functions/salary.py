from pyspark.sql import SparkSession
from pyspark.sql.functions import*
from pyspark.sql.types import*

spark = SparkSession.builder.appName("UDFExample").getOrCreate()

data = [
(101,"Amit",90000),
(102,"Riya",60000),
(103,"John",35000),
(104,"Sara",45000)
]

columns = ["ID","Name","Salary"]

df = spark.createDataFrame(data, columns)

def salary_category(sal):
    if sal>8000:
        return 'High'
    elif sal>=4000:
        return 'Medium'
    else:
        return 'Low'

sal_udf=udf(salary_category, StringType())

df2=df.withColumn('Category', sal_udf('Salary'))
df2.show()
