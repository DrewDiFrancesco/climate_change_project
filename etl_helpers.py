#!/usr/bin/env python3

def generate_url(s3_bucket=None, path_to_files="", file_name=""):
    base_url = "s3://" if s3_bucket else ""
    return f"{base_url}{s3_bucket}/{path_to_files}/{file_name}".rstrip('/')


def read_data(s3_bucket=None, path_to_files="", file_name="", spark=None):
    url = generate_url(s3_bucket, path_to_files, file_name)
    df = spark.read.parquet(url)
    return df


def write_data(df, s3_bucket=None, path_to_files="", file_name="", mode="overwrite", partition_columns=None):
    url = generate_url(s3_bucket=s3_bucket, path_to_files=path_to_files, file_name=file_name)
    if partition_columns:
        df.write.partitionBy(*partition_columns).mode(mode).parquet(url)
    else:
        df.write.mode(mode).parquet(url)