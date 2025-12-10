# Programmatically count the number of columns
# that aren't strings in a DataFrame

from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .appName("Counting number of columns")
    .getOrCreate()
)

exo_2_2_df = spark.createDataFrame(
    [["test", "more test", 10_000_000_000]],["one", "two", "three"]
)

non_string_col_count = sum(1 for column, type in exo_2_2_df.dtypes if type !='string')

print("Non-string columns count: ", non_string_col_count)

