from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("SchemaExample").getOrCreate()

schema=StructType([StructField('id',IntegerType(),True),
StructField('name',StringType(),True),
StructField('address', StructType([StructField('city',StringType(),True),
StructField('pincode',IntegerType(),True)
                                   ]))
                   ])

df=spark.read.json('details.json', schema=schema, multiLine=True)
df.show(truncate=False)