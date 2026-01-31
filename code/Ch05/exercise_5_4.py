#  Exercise 5.4   #############################################################
#
# This program demonstrates various methods to find rows in one DataFrame that 
# do notexist in another DataFrame.
#
###############################################################################

from pyspark.sql import SparkSession

import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

left = spark.createDataFrame([(1, "A"), (2, "B"), (3, "C")], ["my_column", "value_left"])
right = spark.createDataFrame([(3, "X"), (4, "Y"), (5, "Z")], ["my_column", "value_right"])

print("Left DataFrame:")
left.show()

print("Right DataFrame:")
right.show()

print("Rows in 'left' that do not have matching 'my_column' in 'right':")
print("Using left anti join:")
# Perform a left anti join to find rows in 'left' that do not have matching 'my_column' in 'right'
left.join(right, on="my_column", how="left_anti").distinct().show()

print("Using left join with filtering for nulls:")
# Alternative method using left join and filtering for nulls
left.join(right, on="my_column", how="left").where(F.col("value_right").isNull()).select(F.col("my_column"), F.col("value_left")).distinct().show()

print("Using except to find non-matching rows:")
# Alternative method using 'filter' and 'isin' to achieve the same result
right_vals = [row['my_column'] for row in right.select('my_column').collect()]
left.filter(~F.col("my_column").isin(right_vals)).distinct().show()

print("Using except to find non-matching rows:")
# Alternative method using 'where' and 'isin' to achieve the same result
left.where(~F.col("my_column").isin(right_vals)).distinct().show()