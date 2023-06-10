from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

#Creates Empty RDD
emptyRDD = spark.sparkContext.emptyRDD()
#Creates Empty RDD using parallelize
rdd2= spark.sparkContext.parallelize([])

#Create Schema
schema = StructType([
  StructField('firstname', StringType(), True),
  StructField('middlename', StringType(), True),
  StructField('lastname', StringType(), True)
  ])
#Create empty DataFrame from empty RDD
df = spark.createDataFrame(emptyRDD,schema)
df.printSchema()
