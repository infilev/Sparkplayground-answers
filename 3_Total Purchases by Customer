# Initialize Spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

#Copy the starter code or load the file path available in the problem statement 
df = spark.read.format('csv') \
.option('inferSchema', 'true') \
.option('header', 'true') \
.load('/datasets/customer_purchases.csv')

df = df.groupBy('customer_id').agg(sum('purchase_amount').alias('total_purchase')).orderBy('customer_id')
df_result = df.select('customer_id', 'total_purchase')
# Display the final DataFrame using the display() function.
display(df_result)
