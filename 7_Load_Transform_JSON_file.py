# QUESTION
# Problem Statement

# An e-commerce application stores orders as nested JSON files. Each order contains customer details and multiple purchased items.

# Flatten the JSON so that each purchased item becomes a separate row.

# SAMPLE JSON
# [
#   {
#     "order_id":101,
#     "customer":{
#       "id":1,
#       "name":"Rahul"
#     },
#     "items":[
#       {
#         "product":"Laptop",
#         "qty":1,
#         "price":70000
#       },
#       {
#         "product":"Mouse",
#         "qty":2,
#         "price":800
#       }
#     ]
#   } ]

# Task

# Generate the following output.
# order_id	customer_id 	customer_name	 product	qty 	price

# Requirements
# Read JSON.
# Flatten nested struct columns.
# Explode the items array.
# Select only required columns.
# Preserve one row per purchased item.
# Bonus

# Create another column

# item_total = qty × price
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()
df_json = spark.read.option('multiline', 'true').json('/Volumes/workspace/mydb/datafeed/sample.json')
# df_json.printSchema()
df_item_explode = df_json.withColumn('item', explode(col('items')))
# df_item_explode.show()
df_select = df_item_explode.select(
    col("order_id"),
    col("customer.id").alias("customer_id"),
    col("customer.name").alias("customer_name"),
    col("item.product").alias("product"),
    col("item.qty").alias("qty"),
    col("item.price").alias("price")
)

result = df_select.withColumn(
    "item_total",
    col("qty") * col("price")
)


