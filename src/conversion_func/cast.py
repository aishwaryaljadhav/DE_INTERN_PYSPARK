from conv_dataset import df
from pyspark.sql.functions import*

df.select('*',
          col('Salary').cast('int').alias('sal_int'),
col('Rating').cast('double').alias('rating_doub'),
col('Join_Date').cast('date').alias('join_da'),
col('Projects').cast('int').alias('New_project')
          ).show()

df2=df.withColumn("Active", col('Active').cast('boolean')).show()