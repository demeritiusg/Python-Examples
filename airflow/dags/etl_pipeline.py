from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
from datetime import timedelta
import pandas as pd
import requests
import boto3
import psycopg2

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="etl_pipeline",
    description="API + S3 ETL to Postgres",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["etl", "api", "s3", "postgres"],
)
def pipeline_etl():

    @task
    def extract_api():
        url = "https://dummyjson.com/products"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        df = pd.DataFrame(response.json()["products"])[["id", "title", "price"]]
        return df.to_json(orient="records")

    @task
    def extract_s3():
        s3_conn = BaseHook.get_connection("aws_default")
        s3 = boto3.client("s3")
        bucket = "my-sample-bucket"
        key = "data/raw/customers.csv"
        obj = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(obj["Body"])
        return df.to_json(orient="records")

    @task
    def validate_sales(json_data: str):
        df = pd.read_json(json_data)
        required = {"id", "title", "price"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Missing columns: {missing}")
        if df["id"].isnull().any() or (df["price"] <= 0).any():
            raise ValueError("Data validation failed.")
        return df.to_json(orient="records")

    @task
    def transform(sales_json: str, customers_json: str):
        sales_df = pd.read_json(sales_json)
        cust_df = pd.read_json(customers_json)
        merged = sales_df.merge(cust_df, left_on="id", right_on="customer_id", how="inner")
        merged["price_with_tax"] = merged["price"] * 1.07
        return merged.to_json(orient="records")

    @task
    def load_to_postgres(json_data: str):
        df = pd.read_json(json_data)
        conn = psycopg2.connect(
            host="localhost",
            dbname="analytics",
            user="airflow",
            password="airflow",
            port=5432,
        )
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS sales_analytics (
                        id INT PRIMARY KEY,
                        title TEXT,
                        price FLOAT,
                        customer_id INT,
                        customer_name TEXT,
                        price_with_tax FLOAT
                    );
                """)
                for _, row in df.iterrows():
                    cursor.execute(
                        """
                        INSERT INTO sales_analytics
                        (id, title, price, customer_id, customer_name, price_with_tax)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET
                            title = EXCLUDED.title,
                            price = EXCLUDED.price,
                            customer_id = EXCLUDED.customer_id,
                            customer_name = EXCLUDED.customer_name,
                            price_with_tax = EXCLUDED.price_with_tax;
                        """,
                        (row["id"], row["title"], row["price"], row["customer_id"], row["customer_name"], row["price_with_tax"]),
                    )
            conn.commit()
        finally:
            conn.close()

    api_data = extract_api()
    s3_data = extract_s3()
    validated = validate_sales(api_data)
    transformed = transform(validated, s3_data)
    load_to_postgres(transformed)

pipeline_etl()