from pyspark.sql import SparkSession
from pyspark.sql.functions import*

spark=SparkSession.builder.appName("NumericFunctions").getOrCreate()

data = [
(1,"Laptop",55000.75,-5),
(2,"Phone",25000.40,10),
(3,"Tablet",15000.60,-8),
(4,"Monitor",12000.25,15),
(5,"Keyboard",2000.90,-2)
]

columns=["id","product","price","discount"]

df = spark.createDataFrame(data,columns)

df.select(sum('price')).show()

df.select(min('price')).show()

df.select(max('price')).show()

df.select(avg('price')).show()

df.select('product',round('price',1)).show()

df.select('product',abs('discount')).show()

spark.stop()