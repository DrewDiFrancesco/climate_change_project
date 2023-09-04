#!/usr/bin/env python3


def read_data(path_to_files="", file_name="", spark=None):
    data_path = f"{path_to_files}/{file_name}".rstrip('/')
    df = spark.read.parquet(data_path)
    return df


def write_data(df, path_to_files="", file_name="", mode="overwrite", partition_columns=None):
    data_path = f"{path_to_files}/{file_name}".rstrip('/')
    print(f"Saving to: {data_path}")
    if partition_columns:
        df.write.partitionBy(*partition_columns).mode(mode).parquet(data_path)
    else:
        df.write.mode(mode).parquet(data_path)
        
        