from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,LongType, IntegerType


spark = SparkSession \
        .builder \
        .appName("Dataframe Transformation") \
        .getOrCreate()


df =spark.read.format("json").load("../../../data/flight-data/json/2015-summary.json")

#Prints out the schema in the tree format.
df.printSchema()

#Returns the schema of this DataFrame as a pyspark.sql.types.StructType
print(df.schema.simpleString())


#create and Enforce Schema on dataframe

myManualSchema = StructType([
    StructField("DEST_COUNTRY_NAME",StringType(),True),
    StructField("ORIGIN_COUNTRY_NAME",StringType(),True),
    StructField("count",IntegerType(),True)])

df = spark.read.format("json").schema(myManualSchema)\
        .load("../../../data/flight-data/json/2015-summary.json")

df.printSchema()

print(df.schema.simpleString())

spark.stop()