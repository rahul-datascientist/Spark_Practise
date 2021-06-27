from pyspark.sql import SparkSession

#spark = SparkSession.Builder.appName("First_pyspark_program").master("local").getOrCreate()

spark = SparkSession \
    .builder \
    .appName("First_pyspark_program") \
    .getOrCreate()


df = spark.read.format("csv").load("../data/people.csv")

df.show(10,False)

spark.stop()