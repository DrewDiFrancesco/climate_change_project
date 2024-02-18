#!/usr/bin/env python3
import os
from pyspark.sql.functions import *
import schemas

def read_data(path_to_files="", file_name="", spark=None):
    """
    This function reads and returns saved spark dataframes

    Args:
        path_to_files (string): The directory location of where the dataframe you want to read is saved
        file_name (string): The name of the saved dataframe
        spark: The variable that represents the spark session

    Returns:
        df: returns a spark dataframe
    """

    data_path = f"{path_to_files}/{file_name}".rstrip('/')
    df = spark.read.parquet(data_path)
    return df

def create_directory_if_not_exists(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)

def write_data(df, path_to_files="", file_name="", mode="overwrite", partition_columns=None, output_type='parquet',spark_schema=None):
    """
    This function saves a spark dataframe to the given path directory, partitioning the data if given columns to partition on
    
    Args:
        df (spark df): The spark dataframe you want to save
        path_to_files (string): The directory location where you want to save the data
        file_name (string): The name you want the dataframe to be saved as
        mode (string): Can be "overwrite" or "append", depending on whether you wish to save a completely new dataframe or add to an existing one
        partition_columns (list): A list of the columns you wish to partition on
        output_type (string): Either parquet or csv depending on what kind of file you want to save the data as
    """
    file_name = file_name.rstrip('/')
    # data_path = f"{path_to_files}/{file_name}".rstrip('/')
    data_path = os.path.join(path_to_files, file_name)
    print(f"Saving to: {data_path}")

    if output_type == 'parquet':
        schema = getattr(schemas, f'{file_name}_spark')
        if partition_columns:
            df.write.option('schema', schema.json()).partitionBy(*partition_columns).mode(mode).parquet(data_path)
        else:
            df.write.option('schema', schema.json()).mode(mode).parquet(data_path)
            
    elif output_type == 'csv':
        data_path = os.path.join(path_to_files, file_name + '.csv')
        schema = getattr(schemas, f'{file_name}_pandas')
        create_directory_if_not_exists(path_to_files)
        df.toPandas().astype(schema).to_csv(data_path,index=False)
        # df.write.csv(data_path)

def finalize_spark_df_schema(schema, df):
    df_columns = set(df.columns)
    schema_columns = set(field.name for field in schema.fields)
    missing_columns = list(schema_columns - df.columns)
    for column in missing_columns:
        df = df.withColumn(column, lit(None))
    
    columns_from_schema = [
        (col(field.name).cast(field.dataType).alias(field.name))
        for field in schema]
    df = df.select(columns_from_schema)

    return df