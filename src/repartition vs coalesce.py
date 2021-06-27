import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com') \
    .master("local[4]").getOrCreate()

df=spark.range(0,20)
print(df.rdd.getNumPartitions())

df.write.mode("overwrite").csv("../output/partition.csv")

#PySpark DataFrame repartition() method is used to increase or decrease the partitions.
# The below example increases the partitions from 5 to 6 by moving data from all partitions.
df2 = df.repartition(6)
print(df2.rdd.getNumPartitions())

#Spark DataFrame coalesce() is used only to decrease the number of partitions.
# This is an optimized or improved version of repartition() where the movement of the data across the partitions
#  is fewer using coalesce.
df3 = df.coalesce(2)
print(df3.rdd.getNumPartitions())