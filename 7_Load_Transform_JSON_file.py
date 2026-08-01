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


