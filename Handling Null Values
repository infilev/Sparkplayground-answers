# Initialize Spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

#Copy the starter code or load the file path available in the problem statement 
df = spark.read.format('csv') \
.option('inferSchema', 'true') \
.option('header', 'true') \
.load('/datasets/customers_raw.csv') \
# .filter(col('customer_id').isNotNull()  & col('email').isNotNull())



# df=df.na.drop()  


# Display the final DataFrame using the display() function.
display(df)
