from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructField,StructType,StringType,FloatType,IntegerType
# Chạy cục bộ với tất cả các lõi trong CPU
spark= SparkSession.builder.appName("Total_Customer_order").master("local[*]").getOrCreate()
schema = StructType([\
                    StructField("customerID",IntegerType(), True),\
                    StructField("itemID",IntegerType(), True),\
                    StructField("spend",FloatType(), True)])
# Read the file as DF
data = spark.read.schema(schema).csv("customer-orders.csv")
data.show()
#show schema
data.printSchema()
#Sum spend
data.groupby("customerId").agg(func.sum("spend").alias("totalSpend")).show()
# Sort and round
data.groupby("customerId").agg(func.round(func.sum("spend"),2).alias("totalSpend")).sort("totalSpend").show()