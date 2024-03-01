from datetime import datetime 
from airflow.decorators import task 
from airflow.operators.bash import BashOperator 
from airflow.providers.docker.operators.docker import DockerOperator 
from airflow import DAG 

with DAG(
    dag_id = "nyc_taxi", start_date=datetime(2024,1,1), schedule=None
) as dag: 
    system_maintenance = BashOperator(
        task_id = "system_maintenance", 
        bash_command = 'echo "Install some pypi libs.."'
    )

    @task 
    def download_nyc_data_yellow(): 
        import os 
        import numpy as np 
        import pandas as pd 
        import requests 
        data_dir = "opt/airflow/data/"
        os.makedirs(data_dir, exist_ok=True)
        print(os.listdir(data_dir))
        years = ["2022"]
        months = [ "01","02","03","04"]
        data_type = "yellow_tripdata_"
        root_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
        for year in years: 
            for month in months: 
                url_download = root_url + data_type + year + '-' + month + '.parquet'
                print(url_download)
                file_path = os.path.join(
                    data_dir, data_type + year + '-' + month + '.parquet'
                )
                if os.path.exists(file_path): 
                    print("File exists already: ", file_path)
                    continue 
                try: 
                    r = requests.get(url_download, allow_redirects=True)
                    open(file_path, "wb").write(r.content)
                except: 
                    print("Error downloading file: ", url_download)
                    continue 

    @task 
    def download_nyc_data_green(): 
        import os 
        import numpy as np 
        import pandas as pd 
        import requests 
        data_dir = "opt/airflow/data/"
        os.makedirs(data_dir, exist_ok=True)
        print(os.listdir(data_dir))
        years = ["2022"]
        months = [ "01","02","03","04"]
        data_type = "green_tripdata_"
        root_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
        for year in years: 
            for month in months: 
                url_download = root_url + data_type + year + '-' + month + '.parquet'
                print(url_download)
                file_path = os.path.join(
                    data_dir, data_type + year + '-' + month + '.parquet'
                )
                if os.path.exists(file_path): 
                    print("File exists already: ", file_path)
                    continue 
                try: 
                    r = requests.get(url_download, allow_redirects=True)
                    open(file_path, "wb").write(r.content)
                except: 
                    print("Error downloading file: ", url_download)
                    continue 
    
    @task 
    def drop_column(): 
        import os 
        import pandas as pd 
        data_path = "/opt/airflow/data"
        for file in os.listdir(data_path): 
            if file.endswith(".parquet"): 
                df = pd.read_parquet(os.path.join(data_path, file))
                df = df.dropna(axis=1, how="any")
                if "store_and_fwd_flag" in df.columns: 
                    df = df.drop(columns=["store_and_fwd_flag"])
                    df.to_parquet(os.path.join(data_path,file))
                    print('Dropped one column')
                else: 
                    continue 
    @task 
    def drop_missing_data(): 
        import os
        import pandas as pd

        data_path = "/opt/airflow/data/"
        for file in os.listdir(data_path):
            if file.endswith(".parquet"):
                df = pd.read_parquet(os.path.join(data_path, file))
                df = df.dropna()
                # sorted columns
                df = df.reindex(sorted(df.columns), axis=1)
                df.to_parquet(os.path.join(data_path, file))

                print("Dropped missing data from file: " + file)

    @task 
    def transform_data(): 
        import os 
        import pandas as pd
        data_path = "/opt/airflow/data/"
        for file in os.listdir(data_path):
            if file.endswith(".parquet"):
                df = pd.read_parquet(os.path.join(data_path, file))
                if file.startswith("green"):
                    # rename columns tpep_pickup_datetime to pickup_datetime
                    df.rename(
                        columns={
                            "lpep_pickup_datetime": "pickup_datetime",
                            "lpep_dropoff_datetime": "dropoff_datetime",
                        },
                        inplace=True,
                    )
                    df.rename(columns={"ehail_fee": "fee"}, inplace=True)
                    # drop trip_type
                    if "trip_type" in df.columns:
                        df.drop(columns=["trip_type"], inplace=True)
                else:
                    df.rename(columns={"airport_fee": "fee"}, inplace=True)
                    df.rename(
                        columns={
                            "tpep_pickup_datetime": "pickup_datetime",
                            "tpep_dropoff_datetime": "dropoff_datetime",
                        },
                        inplace=True,
                    )
                # lower case all columns
                df.columns = map(str.lower, df.columns)
                # drop fee column
                if "fee" in df.columns:
                    df.drop(columns=["fee"], inplace=True)
                df.to_parquet(os.path.join(data_path, file))


    @task
    def fix_data_type():
        import os

        import pandas as pd

        data_path = "/opt/airflow/data/"
        for file in os.listdir(data_path):
            if file.endswith(".parquet"):
                df = pd.read_parquet(os.path.join(data_path, file))
                # convert payment_type to int
                if "payment_type" in df.columns:
                    df["payment_type"] = df["payment_type"].astype(int)
                df.to_parquet(os.path.join(data_path, file))


    @task
    def create_streamming_data():
        import os

        import pandas as pd

        data_path = "/opt/airflow/data/"
        streamming_path = os.path.join(data_path, "stream")
        os.makedirs(streamming_path, exist_ok=True)
        df_list = []
        for file in os.listdir(data_path):
            df = pd.read_parquet(os.path.join(data_path, file))
            # get random 1000 rows
            # drop na
            df = df.dropna()
            print(df.shape)
            if df.shape[0] < 10000:
                continue
            df = df.sample(n=10000)
            df["content"] = [file.split("_")[0]] * 10000
            df_list.append(df)
        df = pd.concat(df_list)
        df.to_parquet(os.path.join(streamming_path, "stream.parquet"))


system_maintenance >> [download_nyc_data_yellow(), download_nyc_data_green()] >> fix_data_type() >> transform_data() >> drop_missing_data() >> create_streamming_data()