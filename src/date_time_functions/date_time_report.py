from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("EcommerceReport").getOrCreate()

data = [
(101,"Aishwarya","Laptop","2026-03-01"),
(102,"Rahul","Mobile","2026-03-03"),
(103,"Sneha","Headphones","2026-04-10"),
(104,"Arjun","Tablet","2026-04-15")
]

columns = ["Order_ID","Customer","Product","Order_Date"]

df = spark.createDataFrame(data,columns)

df.select(
    'Order_ID', 'Order_Date',
    current_date(),
    current_timestamp(),
    year('Order_Date').alias('Year'),
    month('Order_Date').alias('Month'),
    dayofmonth('Order_Date').alias('day'),
    date_format('Order_Date', 'dd-MM-yyyy').alias('formatted_date')
).show(truncate=False)


