from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Transformations_Actions_Demo") \
    .master("local[*]") \
    .getOrCreate()

data = [
    ("Alice", 25, "IT"),
    ("Bob", 30, "HR"),
    ("Charlie", 35, "IT"),
    ("David", 28, "Finance"),
    ("Eva", 22, "HR")
]

df = spark.createDataFrame(data, ["Name", "Age", "Department"])

print("Original Data")
df.show()


df_filter = df.filter(df.Age > 25)

df_select = df_filter.select("Name", "Department")

df_group = df.groupBy("Department").count()


print("Filtered Data (Age > 25)")
df_filter.show()

print("Selected Columns")
df_select.show()

print("Grouped Data")
df_group.show()

print("Total Number of Records")
print(df.count())


spark.stop()