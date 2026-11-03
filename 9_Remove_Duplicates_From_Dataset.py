# Problem Statement
# You are working as a Data Engineer for an e-commerce company. The company receives customer information from multiple source systems, and the same customer can appear multiple times in the dataset.

# Your task is to identify and remove duplicate customer records based on customer_id, keeping only the most recently updated record for each customer.
# Table: customers
# customer_id	customer_name	city	last_updated
# 101	Rahul	Delhi	2024-01-02
# 101	Rahul	Mumbai	2024-02-10
# 102	Amit	Pune	2024-03-01
# 102	Amit	Pune	2024-02-01
# 103	Neha	Hyderabad	2024-01-15
# 103	Neha	Bangalore	2024-03-20
# 104	Priya	Chennai	2024-02-05
# 104	Priya	Chennai	2024-02-05
# 105	Karan	Kolkata	2024-04-01

# Tasks
# 1. Identify duplicate records using customer_id.
# 2. Keep only the latest record based on last_updated.
# 3. Remove older records.
# 4. Return the cleaned dataset with all original columns.
# Expected Output
# customer_id	customer_name	city	last_updated
# 101	Rahul	Mumbai	2024-02-10
# 102	Amit	Pune	2024-03-01
# 103	Neha	Bangalore	2024-03-20
# 104	Priya	Chennai	2024-02-05
# 105	Karan	Kolkata	2024-04-01




from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Remove Duplicates").getOrCreate()
customer = spark.createDataFrame(
    [(1,'Rahul','Delhi','2024-01-02'),
    (1,'Rahul','Mumbai','2024-02-10'),
    (2,'Amit','Pune','2024-03-01'),
    (2,'Amit','Pune','2024-02-01'),
    (3,'Neha','Hyderabad','2024-01-15'),
    (3,'Neha','Bangalore','2024-03-20'),
    (4,'Priya','Chennai','2024-02-05'),
    (4,'Priya','Chennai','2024-02-05'),
    (5,'Karan','Kolkata','2024-01-01'),
    (5,'Karan','Kolkata','2024-01-01')],
    ['customer_id','customer_name','city', 'last_updated'])

window = Window.partitionBy('customer_id').orderBy(
    F.col('last_updated').desc()
)
df_new = customer.withColumn('rank_col', F.row_number().over(window))  
df_new= df_new.filter(F.col('rank_col')==1)  
df_new = df_new.select('customer_id','customer_name','city','last_updated').drop('rank_col')
df_new.show()


# What if two records have the same last_updated timestamp?
# For deterministic results, add another column as a tie-breaker, such as updated_at_timestamp, ingestion_timestamp, or a unique record ID.

window = Window.partitionBy('customer_id').orderBy(
    F.col('last_updated').desc(),
    F.col('ingestion_timestamp').desc()
)



# PySpark — Without Window Function
# Step 1: Find the latest date for each customer.
# from pyspark.sql import functions as Flatest_dates = (    customer    .groupBy('customer_id')    .agg(        F.max('last_updated').alias('last_updated')    ))

# Step 2: Join the original DataFrame with the latest dates.
# df_new = customer.join(    latest_dates,    on=['customer_id', 'last_updated'],    how='inner')

# Step 3: Select required columns.
# df_new = df_new.select(    'customer_id',    'customer_name',    'city',    'last_updated')df_new.show()



