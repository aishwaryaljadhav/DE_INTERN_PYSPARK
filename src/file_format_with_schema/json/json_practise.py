from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("SchemaExample").getOrCreate()

schema=StructType([StructField('id',IntegerType(),True),
                    StructField('name',StringType(),True),
                    StructField('marks',IntegerType(),True)
                   ])

df=spark.read.json('students.json',schema=schema)

df.show()
df.printSchema()