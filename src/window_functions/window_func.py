from employee_dataset import df
from pyspark.sql.functions import*
from pyspark.sql.window import Window

window_spec=Window.partitionBy('Department').orderBy(col('Salary').desc())

df2=df.withColumn('Ranks',rank().over(window_spec))\
.withColumn('dense_rank',dense_rank().over(window_spec))\
.withColumn('row_number',row_number().over(window_spec))\
.withColumn('next_sal',lead('Salary').over(window_spec))\
.withColumn('prev_salary',lag('Salary').over(window_spec)).show()