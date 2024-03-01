import os 
from datetime import datetime 
import pandas as pd 
from airflow.decorators import task 
from airflow.operators.python import PythonOperator 
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator
)
from airflow import DAG 

data_dir = "opt/airflow/data/"
postgres_conn_id = "postgres_default"
def insert_table(): 
    pg_hook = PostgresHook.get_hook(conn_id = postgres_conn_id)
    for file in os.listdir(data_dir): 
        if file.endswith(".parquet") and str(file).startswith("yello"): 
            df = pd.read_parquet(os.path.join(data_dir,file))
            print("Inserting data from file: ", file)
            pg_hook.insert_rows(table="taxi_warehouse", rows=df.values.tolist())

with DAG(dag_id="nyc_taxi2", start_date=datetime(2024,1,1), schedule=None) as dag: 
    create_table_query = PostgresOperator(
        task_id = "create_table_pg", postgres_conn_id=postgres_conn_id, 
        sql = """
        CREATE TABLE IF NOT EXISTS taxi_warehouse(
                      vendorid  INT, 
            pickup_datetime TIMESTAMP WITHOUT TIME ZONE, 
            dropoff_datetime TIMESTAMP WITHOUT TIME ZONE, 
            passenger_count FLOAT, 
            trip_distance FLOAT, 
            ratecodeid FLOAT, 
            store_and_fwd_flag VARCHAR(1), 
            pulocationid INT, 
            dolocationid INT, 
            payment_type INT, 
            fare_amount FLOAT, 
            extra FLOAT, 
            mta_tax FLOAT, 
            tip_amount FLOAT, 
            tolls_amount FLOAT, 
            improvement_surcharge FLOAT, 
            total_amount FLOAT, 
            congestion_surcharge FLOAT  
        );
        """
        
    )
    insert_table_pg = PythonOperator(
            task_id = "insert_table_pg", 
            python_callable= insert_table
        )
    gx_validate_pg = GreatExpectationsOperator(
        task_id = "gx_validation_pg", 
        conn_id = postgres_conn_id, 
        data_context_root_dir="include/great_expectations", 
        data_asset_name="public.nyc_taxi", 
        database= "helen", 
        expectation_suite_name="taxi_suite", 
        return_json_dict=True
    )

create_table_query >> insert_table_pg >> gx_validate_pg