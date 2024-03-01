import os

import pandas as pd

data_dir = "data/2022"
data_type1 = "green_tripdata_"
data_type2 = "yellow_tripdata_"
root = "data/"
months = ["01", "02", "03", "04"]

list_parquet1 = [
    os.path.join(data_dir, data_type1 + "2022-" + month + ".parquet")
    for month in months
]
list_parquet2 = [
    os.path.join(data_dir, data_type2 + "2022-" + month + ".parquet")
    for month in months
]
list_df1 = [pd.read_parquet(parquet) for parquet in list_parquet1]
list_df2 = [pd.read_parquet(parquet) for parquet in list_parquet2]
df1 = pd.concat(list_df1)
df2 = pd.concat(list_df2)
df1.to_parquet(root + "/green_tripdata_2022.parquet")
df2.to_parquet(root + "/yellow_tripdata_2022.parquet")
# check missing values
print(df1.isnull().sum())