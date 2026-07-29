#Initialize Spark session

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()


products = spark.createDataFrame([
    (1, "Apple Juice",        "Beverages"),
    (2, "Orange Juice",       "Beverages"),
    (3, "Chocolate Bar",      "Snacks"),
    (4, "Potato Chips",       "Snacks"),
    (5, "Fresh Strawberries", "Fruits"),
    (6, "Sparkling Water",    "Beverages"),
], ["product_id", "name", "category"])

sales = spark.createDataFrame([
    (1, 1, 10, 20),
    (2, 1,  5, 10),
    (3, 2,  8, 16),
    (4, 3,  2,  4),
    (5, 4, 15, 30),
    (6, 4,  5, 10),
    (7, 6, 12, 24),
], ["sale_id", "product_id", "quantity", "revenue"])

inventory = spark.createDataFrame([
    (1, 50, "Warehouse A"),
    (2, 40, "Warehouse A"),
    (2, 20, "Warehouse B"),
    (3, 30, "Warehouse A"),
    (4, 20, "Warehouse A"),
    (4, 15, "Warehouse B"),
    (5, 10, "Warehouse A"),
], ["product_id", "stock", "warehouse"])


#Copy the starter code or load the file path available in the problem statement 
df_sales = sales.groupBy('product_id').agg(
  sum('quantity').alias('total_quantity'),
  sum('revenue').alias('total_revenue')
)

df_inventory = inventory.groupBy('product_id').agg(
  sum('stock').alias('total_stock')
)

df_join = products \
.join(df_sales, on = 'product_id', how = 'left') \
.join(df_inventory, on = 'product_id', how = 'left')

df_fill = df_join.fillna(
  {
    'total_quantity': 0,
    'total_revenue': 0,
    'total_stock': 0
  }
)

df_result = df_fill.select('product_id', 'name', 'category', 
                           'total_quantity', 'total_revenue', 'total_stock') \
                           .orderBy(col('product_id').asc())
df_result.show()
