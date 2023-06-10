import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName('Spark').getOrCreate()
spark1 = SparkSession.builder.appName('Spark1').getOrCreate()

data = [('James', '', 'Smith', '1991-04-01', 'M', 3000),
        ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
        ('Robert', '', 'Williams', '1978-09-05', 'M', 4000),
        ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
        ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1)
        ]

columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
df = spark.createDataFrame(data=data, schema=columns)
df.printSchema()
df.show(truncate=False)

df.createOrReplaceTempView("table_df")
df.createOrReplaceGlobalTempView("table_df")
#spark.catalog.dropTempView('table_df')
spark.catalog.dropTempView('table_df')

#spark.sql("select * from table_df").show()
spark1.sql("select * from global_temp.table_df").show()
spark1.sql("select * from table_df").show()
