from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName("Dataframe Transformation") \
        .getOrCreate()

#Returns the schema of this DataFrame as a pyspark.sql.types.StructType.
spark.read.format("json").load("../../../data/flight-data/json/2015-summary.json").schema


spark.stop()