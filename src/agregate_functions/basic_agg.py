from pyspark.sql.functions import *
from sales_dataset import df

df.groupBy('Category').agg(
    sum('Price').alias('Total_sales')
                           ).show()

df.groupBy('Product').agg(
    avg('Price').alias('Average_Price')
                           ).show()

df.groupBy('Product').agg(
    max('Price').alias('max_price')
                           ).show()

df.groupBy('Product').agg(
    min('Price').alias('min_price')
                           ).show()

df.groupBy('Customer').agg(
    count('OrderID').alias('Total_orders')
                           ).show()