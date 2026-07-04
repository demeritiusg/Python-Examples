from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, year, udf
from pyspark.sql.types import StringType

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

file_path = "data/input_data.csv"



def spark_transform(file_path):
       
    output_file_path = "data/output_data.parquet"
    
    
    spark = SparkSession.builder \
        .appName("DataPipelineExample") \
        .getOrCreate()

    try:
        input_data_df = spark.read.csv(file_path, header=True, inferSchema=True)
        count = input_data_df.count()
        logger.info(f"Number of records read from source: {count}")

        transformed_data_df = input_data_df \
            .withColumn("new_column", when(col("old_column") > 0, "positive").otherwise("non-positive"))
        
        transformed_data_df_count = transformed_data_df.count()
        logger.info(f"Number of records after transformation: {transformed_data_df_count}")
    except Exception as e:
        logger.error(f"Error during transformation: {e}")
        raise

    try:
        transformed_data_df.write.mode("overwrite").parquet(output_file_path)
    except Exception as e:
        logging.error(f"Error writing to Parquet: {e}")
        raise
    
    return transformed_data_df

#----------------------------------------------------------

# Create a SparkSession
def spark_filtering(file_path):
    spark = SparkSession.builder \
        .appName("DataManipulationExample") \
        .getOrCreate()

    # Read data from CSV file into a DataFrame
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
    other_df = spark.read.csv(file_path, header=True, inferSchema=True)
    joined_df = df.join(other_df, df["id"] == other_df["id"], "inner")

    # Apply a UDF (User Defined Function)

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
    
    
    return filtered_df, df_with_new_column, grouped_df, sorted_df, joined_df, df_with_udf, df_with_year

# Show the results
def main(input_file_path):
    spark_transform(input_file_path)
    spark_filtering(input_file_path)
    # filtered_df.show()
    # df_with_new_column.show()
    # grouped_df.show()
    # sorted_df.show()
    # joined_df.show()
    # df_with_udf.show()
    # df_with_year.show()
if __name__ == '__main__':
    main()


