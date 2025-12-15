from pyspark.sql import SparkSession
import pyspark.sql.functions as F


spark = SparkSession.builder.appName(
    "Counting word occurences from a book."
).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

def count_word_occurences(file_path):
    # Count word occurences in the given file
    results = (
        spark.read.text(file_path)
        .select(F.split(F.col("value"), " ").alias("line"))
        .select(F.explode(F.col("line")).alias("word"))
        .select(F.lower(F.col("word")).alias("word"))
        .select(F.regexp_extract(F.col("word"), "[a-z']*", 0).alias("word"))
        .where(F.col("word") != "")
        .groupby(F.col("word"))
        .count()
    )
    return results

# If you need to read multiple text files, replace `1342-0` by `*`.
file_path = "../../data/gutenberg_books/1342-0.txt"

# Display the top 5 most popular first letters
print("Top 5 most popular first letters:")
(
    count_word_occurences(file_path)
    .select(
        F.col("word").substr(1, 1).alias("first_letter"),
        F.col("count")         
    )
    .groupby("first_letter")
    .agg(F.sum("count").alias("total_count"))
    .orderBy(F.col("total_count").desc())
    .show(5)
)

# Display the number of words starting with consonants and vowels
print("Number of words starting with consonants and vowels:")
(
    count_word_occurences(file_path)
    .select(
        F.when(
            F.col("word").substr(1, 1).rlike("^[aeiou]"),
            "vowel"
        ).otherwise("consonant").alias("letter_type"),
        F.col("count")
    )
    .groupby("letter_type")
    .agg(F.sum("count").alias("total_count"))
    .show()
)
