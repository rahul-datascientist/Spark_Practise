from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,LongType, IntegerType
from pyspark.sql.functions import  Column,expr

import pyspark.sql.functions as psf


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

#show all coluns and datatype
print(df.columns)

print(df.dtypes)

df.createOrReplaceTempView("dfTable")

##Data operations

#select and selectExpr
df.select("ORIGIN_COUNTRY_NAME","DEST_COUNTRY_NAME").show(2,False)

spark.sql("select ORIGIN_COUNTRY_NAME,DEST_COUNTRY_NAME from dfTable limit 2");

df.select("ORIGIN_COUNTRY_NAME","DEST_COUNTRY_NAME").show(2,False)

spark.sql("select ORIGIN_COUNTRY_NAME,DEST_COUNTRY_NAME from dfTable limit 2");

spark.stop()