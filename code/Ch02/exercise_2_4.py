from pyspark.sql import SparkSession
from pyspark.sql.functions import col, greatest

spark = SparkSession.builder.getOrCreate()
exo2_4_df = spark.createDataFrame(
    [["key1", 10_000, 20_000], ["key2", 15_000, 5_000]], ["key", "value1", "value2"]
)

exo2_4_df.printSchema()

# Find the greatest value between 'value1' and 'value2' for each row
exo2_4_df.select(
    greatest(col("value1"), col("value2")).alias("greatest_value")
).show()