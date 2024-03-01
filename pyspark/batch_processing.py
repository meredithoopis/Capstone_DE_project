import argparse
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession 
from pydeequ.profiles import ColumnProfilerRunner

load_dotenv(".env")

def main(): 
    spark = (
        SparkSession.builder.master("local[*]")
        .config(
            "spark.jars",
            "jars/postgresql-42.4.3.jar,jars/deequ-2.0.3-spark-3.3.jar,jars/trino-jdbc-434.jar",
        )
        .appName("Python Spark")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()
    )
    print("Spark session created")
    df = (
        spark.read.format("jdbc")
        .option("driver", "io.trino.jdbc.TrinoDriver")
        .option("url", f"jdbc:trino://localhost:{os.getenv('TRINO_PORT')}/")
        .option(
            "dbtable", os.getenv("TRINO_DBTABLE", "datalake.taxi_time_series.nyc_taxi")
        )
        .option("user", os.getenv("TRINO_USER", "trino"))
        .option("password", os.getenv("TRINO_PASSWORD", ""))
        .load()
    )
    columns = df.columns
    print(columns)
    print(df.groupBy("payment_type").count().show())


if __name__ == "__main__": 
    main()