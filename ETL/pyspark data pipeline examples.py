from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Create a SparkSession
spark = SparkSession.builder \
    .appName("DataPipelineExample") \
    .getOrCreate()

# Define input file path
input_file_path = "data/input_data.csv"

# Read input data into a DataFrame
input_data_df = spark.read.csv(input_file_path, header=True, inferSchema=True)

# Perform data transformation
transformed_data_df = input_data_df \
    .withColumn("new_column", when(col("old_column") > 0, "positive").otherwise("non-positive"))

# Define output file path
output_file_path = "data/output_data.parquet"

# Write transformed data to output file in Parquet format
transformed_data_df.write.mode("overwrite").parquet(output_file_path)

# Stop the SparkSession
spark.stop()

#----------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, year

# Create a SparkSession
spark = SparkSession.builder \
    .appName("DataManipulationExample") \
    .getOrCreate()

# Read data from CSV file into a DataFrame
file_path = "data/input_data.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Filter data for a specific condition
filtered_df = df.filter(col("age") > 30)

# Add a new column based on a condition
df_with_new_column = df.withColumn("age_category", when(col("age") < 18, "Child").when(col("age").between(18, 64), "Adult").otherwise("Senior"))

# Group by a column and perform aggregation
grouped_df = df.groupBy("gender").agg({"salary": "avg"})

# Sort the DataFrame by a column
sorted_df = df.orderBy("age", ascending=False)

# Perform a join operation
other_df = spark.read.csv("data/other_data.csv", header=True, inferSchema=True)
joined_df = df.join(other_df, df["id"] == other_df["id"], "inner")

# Apply a UDF (User Defined Function)
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def get_age_category(age):
    if age < 18:
        return "Child"
    elif age >= 18 and age <= 64:
        return "Adult"
    else:
        return "Senior"

age_category_udf = udf(get_age_category, StringType())
df_with_udf = df.withColumn("age_category", age_category_udf(col("age")))

# Extract year from date column
df_with_year = df.withColumn("year_of_birth", year(col("dob")))

# Show the results
filtered_df.show()
df_with_new_column.show()
grouped_df.show()
sorted_df.show()
joined_df.show()
df_with_udf.show()
df_with_year.show()

# Stop the SparkSession
spark.stop()

