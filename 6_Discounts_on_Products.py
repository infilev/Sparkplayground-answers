# THIS QUESTION IS MADE BY ME AS IT IS PREMIUM QUESTION
# QUESTION

# Problem Statement

# You are given a products table containing product details. Calculate the discounted selling price for each product.

# Table: products 

# Column	          Type
# product_id	  INT
# product_name	  STRING
# category	  STRING
# price	          STRING
# discount_percent  INT

# Sample Data

# product_id	product_name	category	price	discount_percent
# 1	           Laptop	Electronics	"85000"	  10
# 2	           Shoes	Fashion	        "5000"	  20
# 3	           Watch	Accessories	"12000"	  15
# 4	           Phone        Electronics	"45000"	  5
# Task

# Create a new DataFrame that contains:
# product_id
# product_name
# original_price
# discount_amount
# final_price

# Requirements
# 1) Convert price from string to numeric.
# 2) Calculate 
# discount_amount = price × discount_percent / 100
# 3) Calculate
# final_price = price − discount_amount
# 4) Round the final price to 2 decimal places.




# SOLUTION
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

schema_type = StructType([
  StructField('product_id', IntegerType(), False),
  StructField('product_name', StringType(), True),
  StructField('category', StringType(), True),
  StructField('price', StringType(), True),
  StructField('discount_percent', IntegerType(), True)
]
)

data = [
  (1, 'Laptop', 'Electronics', "85000", 10),
  (2, 'shoes', 'Fashion', "5000", 20),
  (3, 'watch', 'Accessories', "12000", 15),
  (4, 'phones', 'Electronics', "45000", 5)
]

df = spark.createDataFrame(data, schema=schema_type)
# products.show(3)

df1 = df.withColumn(
    "original_price",
    col("price").cast("double")
)
df2 = df1.withColumn(
    "discount_amount",
    col("original_price") * col("discount_percent") / 100
)
df3 = df2.withColumn(
    "final_price",
    col("original_price") - col("discount_amount")
)
result = df3.select(
    "product_id",
    "product_name",
    "original_price",
    round(col("discount_amount"), 2).alias("discount_amount"),
    round(col("final_price"), 2).alias("final_price")
)

result.show()


# result = (
#     df.withColumn("price", col("price").cast("double"))
#       .withColumn("discount_amount", col("price") * col("discount_percent") / 100)
#       .withColumn("final_price", col("price") - col("discount_amount"))
#       .withColumn("discount_amount", round(col("discount_amount"), 2))
#       .withColumn("final_price", round(col("final_price"), 2))
# )
