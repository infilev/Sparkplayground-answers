# Problem Statement

# A company wants to identify employees earning more than the average salary of their department.

# Table: Employees
# employee_id	employee_name	department_id	salary
# Table: Departments

# | department_id | department_name |

# Sample Data

# Employees

# employee_id	employee_name	department_id	salary
# 1	             Rahul	             10	        70000
# 2	             Amit	             10	        60000
# 3          	 Neha	             20	        80000
# 4	             Pooja	             20	        65000
# 5	             Ravi	             20	90000

# Departments

# department_id	department_name
# 10	               IT
# 20	               HR
# Task

# Return employees whose salary is greater than the average salary of their own department.

# Output Columns
# employee_id
# employee_name
# department_name
# salary
# department_average_salary
# salary_difference

# where

# salary_difference = salary - department_average_salary
# Constraints
# Use a Window Function to compute department averages.
# Join with the Departments table.
# Do not use subqueries for average calculation.

# Expected Difficulty
# Hard



from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Employee Earnings").getOrCreate()
department = spark.createDataFrame(
    [(10,'IT'),(20,'HR')],
    ['department_id','department_name']
    )
employee = spark.createDataFrame(
    [(1,'Rahul',10,70000),(2,'Amit',10,60000),(3,'Neha',20,80000),(4,'Pooja',20,65000),(5,'Ravi',20,90000)],
    ['employee_id','employee_name','department_id','salary']
    )
windowfunc = Window.partitionBy('department_id')
df_join = employee.join(department, on = 'department_id', how = 'inner')
df_join = df_join.withColumn('dept_avg_salary',avg('salary').over(windowfunc))
df_join = df_join.withColumn('salary_difference', col('salary') - col('dept_avg_salary'))
df_join = df_join.filter(col('salary')> col('dept_avg_salary'))
df_join.select('employee_id','employee_name','department_name','salary','dept_avg_salary','salary_difference').show()





# SQL SOLUTION 

WITH department_averages AS (
    SELECT
        department_id,
        AVG(salary) AS department_average_salary
    FROM Employees
    GROUP BY department_id
)
SELECT
    e.employee_id,
    e.employee_name,
    d.department_name,
    e.salary,
    da.department_average_salary,
    e.salary - da.department_average_salary AS salary_difference
FROM Employees e
JOIN Departments d
    ON e.department_id = d.department_id
JOIN department_averages da
    ON e.department_id = da.department_id
WHERE e.salary > da.department_average_salary;




# Without window function


from pyspark.sql import functions as F

department_averages = (
    employees_df
    .groupBy("department_id")
    .agg(
        F.avg("salary").alias("department_average_salary")
    )
)

result = (
    employees_df
    .join(department_averages, on="department_id", how="inner")
    .join(departments_df, on="department_id", how="inner")
    .filter(
        F.col("salary") > F.col("department_average_salary")
    )
    .withColumn(
        "salary_difference",
        F.col("salary") - F.col("department_average_salary")
    )
    .select(
        "employee_id",
        "employee_name",
        "department_name",
        "salary",
        "department_average_salary",
        "salary_difference"
    )
)

result.show()
