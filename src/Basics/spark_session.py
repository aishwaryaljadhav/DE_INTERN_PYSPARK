from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("MyApp") \
        .master("local[*]") \
        .getOrCreate()

    print("Spark Session Running")

    spark.stop()


if __name__ == "__main__":
    main()