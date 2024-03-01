import os 
from dotenv import load_dotenv 
from pydeequ.analyzers import AnalysisRunner, AnalyzerContext, Completeness, Size 
from pydeequ.checks import * 
from pydeequ.profiles import ColumnProfilerRunner 
from pydeequ.suggestions import * 
from pydeequ.verification import * 
from pyspark.sql import SparkSession 

load_dotenv(".env")

def main(): 
    spark = (
        SparkSession.builder.master("local[*]")
        .config("spark.jars", "./jars/postgresql-42.6.0.jar,./jars/deequ-2.0.3-spark-3.3.jar")
        .appName("Python Spark").getOrCreate()
    )
    df = (
        spark.read.format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", f"jdbc:postgresql:{os.getenv('POSTGRES_DB')}")
        .option("dbtable", "public.services")
        .option("user", os.getenv("POSTGRES_USER"))
        .option("password", os.getenv("POSTRGES_PASSWORD"))
        .load()
    )
    profile_res = ColumnProfilerRunner(spark).onData(df).run()
    for col, profile in profile_res.profiles.items(): 
        if col == "index": 
            continue 
        print("*" * 40)
        print(f"Column: {col}")
        print(profile)

    analysis_res = AnalysisRunner(spark).onData(df).addAnalyzer(Size()).addAnalyzer(Completeness("velocity")).run()
    analysis_res_df = AnalyzerContext.successMetricsAsDataFrame(
        spark, analysis_res
    )
    analysis_res_df.show()
    #Checking 
    check = Check(spark, CheckLevel.Error, "Review Check")
    checkresult = VerificationSuite(spark).onData(df).addCheck(check.isComplete("velocity").isNonNegative("velocity")).run()
    check_res_df = VerificationResult.checkResultsAsDataFrame(spark, checkresult)
    check_res_df.show()

if __name__ == "__main__": 
    main()