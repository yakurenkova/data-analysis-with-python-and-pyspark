# Rewrite a code snippet, removing the withColumnRenamed method

from pyspark.sql import SparkSession
from pyspark.sql.functions import length, col

spark = (
    SparkSession
    .builder
    .appName("Counting number of columns")
    .getOrCreate()
)

exo_2_3_df = (
    spark.read.text("data/gutenberg_books/1342-0.txt")
    .select(length(col("value")).alias("number_of_char"))
)

exo_2_3_df.show(10)

