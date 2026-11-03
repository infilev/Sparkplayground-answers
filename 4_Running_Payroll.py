# We will join the employees and payroll on employee_id.
# Then calculate the pay using when() and otherwise().
# AT last select the required output columns.


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

# Assume the dataframes employees, payroll are already initialized.
employees = spark.createDataFrame([
    (1, "Alice", 25, "Software Engineer"),
    (2, "Bob", 30, "Data Analyst"),
    (3, "Carol", 28, "Product Manager"),
    (4, "Dave", 24, "Software Engineer")
], ["employee_id", "name", "age", "position"])


payroll = spark.createDataFrame([
    (1, 45.0, 30.0),
    (2, 38.0, 25.0),
    (3, 41.5, 35.0),
    (4, 40.0, 28.0)
], ["employee_id", "hours_worked", "hourly_rate"])

# Write the logic and display the final dataframe
result_join = employees.join(payroll, on = 'employee_id', how ='inner')

df_trans = result_join.withColumn(
  "pay", 
  F.when
  (
    F.col('hours_worked')<=40, F.col("hours_worked")*F.col('hourly_rate')
  ).otherwise
  (
    (40*F.col('hourly_rate')) + ((F.col('hours_worked') - 40) * F.col('hourly_rate') *1.5)
  )
) 
df_result = df_trans.select('employee_id', 'name', 'pay', 'position')
df_result.show(truncate=False)
    
