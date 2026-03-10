from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("DeliveryTracking").getOrCreate()

data = [
(101,"Laptop","2026-03-01","2026-03-05"),
(102,"Mobile","2026-03-03","2026-03-08"),
(103,"Tablet","2026-03-04","2026-03-09"),
(104,"Headphones","2026-03-05","2026-03-06")
]

columns = ["Order_ID","Product","Order_Date","Delivery_Date"]

df = spark.createDataFrame(data,columns)

df2= df.withColumn('Order_Date', to_date("Order_Date")) \
       .withColumn('Delivery_Date', to_date("Delivery_Date"))

df2.select(
    'Order_Date',
    date_add('Order_Date',5).alias("Expected Delivery"),
    datediff('Delivery_Date',"Order_Date").alias("Delivery days")
).show()
