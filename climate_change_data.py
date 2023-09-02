#!/usr/bin/env python3

import pandas as pd
import os
import xlrd
from spark_session import SparkManager
import config as config
import numpy as np
from conf_variables import default_args
from pyspark.sql.types import StructType, StructField, StringType
import etl_helpers as etl_helpers



def main(spark=None, override_args=None):

    print(f"HERE is the override args: {override_args}")

    if override_args:
        default_args.update(override_args)

    print(f"HERE is the new default args: {default_args}")
    config_manager = config.Config(default_args)

    if spark is None:
        spark = SparkManager(config_manager.args).get_spark()

    data_path = config_manager.args['data_path']

    # Specify the path to your Excel file
    file_path = f'{data_path}/climate_change_download_0.xls'

    # Read the Excel file
    xls = pd.ExcelFile(file_path)

    # Get the list of sheet names (tabs)
    sheet_names = xls.sheet_names

    # Read each sheet into a DataFrame
    dfs = []
    for sheet_name in sheet_names:
        df = pd.read_excel(xls, sheet_name)
        dfs.append(df)

    # Access the DataFrames for each sheet
    data = dfs[0]  # First sheet
    country = dfs[1]  # Second sheet
    series = dfs[2]  # Third sheet

    # Columns to keep as identifiers
    id_columns = ['Country code', 'Country name', 'Series code', 'Series name', 'SCALE', 'Decimals']

    # Melt the DataFrame, keeping the identified columns as id_vars
    melted_df = data.melt(id_vars=id_columns, var_name='Year', value_name='Value')

    merged_df = melted_df.merge(country, on=['Country code', 'Country name'], how='left')

    # Columns to keep from series_info_df
    columns_to_keep = ['Series name', 'Series code', 'Topic', 'Definition']

    # Merge the two DataFrames on 'series code' and 'series name'
    final_df = merged_df.merge(series[columns_to_keep], on=['Series code', 'Series name'], how='left')

    # Filter for non-aggregated data
    non_aggregated_df = final_df[final_df['Region'] != 'Aggregates']

    # Filter for aggregated data
    aggregated_df = final_df[final_df['Region'] == 'Aggregates']


    income_level_country_codes = ['High income', 'Low income', 'Lower middle income', 'Low & middle income', 
                                'Middle income', 'Upper middle income']

    # List of unique country codes related to region
    region_country_codes = ['East Asia & Pacific', 'Europe & Central Asia', 'Euro area', 'Latin America & Caribbean',
                            'Middle East & North Africa', 'South Asia', 'Small island developing states',
                            'Sub-Saharan Africa', 'World']

    # Filter aggregated data based on income level country codes
    income_level_df = aggregated_df[aggregated_df['Country name'].isin(income_level_country_codes)]

    # Filter aggregated data based on region country codes
    region_df = aggregated_df[aggregated_df['Country name'].isin(region_country_codes)]

    table_list = [non_aggregated_df, income_level_df, region_df]
    # count = 0
    # for table in table_list:
    #     # try:
    #     table['Value'] = table['Value'].astype(str)
    #     table['Value'] = table['Value'].apply(lambda x: x.replace('5% / 25%; BY: 2000', '..'))
    #     table.loc[table['Value'] == '..', 'Value'] = np.nan
    #     table['Value'] = table['Value'].astype(float)
        # except:
        #     print(f"error with table {count}")
        # count += 1

    schema = StructType([
    StructField("Country code", StringType(), True),
    StructField("Country name", StringType(), True),
    StructField("Series code", StringType(), True),
    StructField("Series name", StringType(), True),
    StructField("SCALE", StringType(), True),
    StructField("Decimals", StringType(), True),
    StructField("Year", StringType(), True),
    StructField("Value", StringType(), True),
    StructField("Capital city", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("Income group", StringType(), True),
    StructField("Lending category", StringType(), True),
    StructField("Topic", StringType(), True),
    StructField("Definition", StringType(), True)
    ])

    # print(aggregated_df)
    s_income_level_df = spark.createDataFrame(income_level_df, schema)
    s_region_df = spark.createDataFrame(region_df, schema)
    s_non_aggregated_df = spark.createDataFrame(non_aggregated_df, schema)

    etl_helpers.write_data(s_income_level_df, s3_bucket=config_manager.args['s3_bucket'], path_to_files=config_manager.args['data_path'], file_name="agg_income_level", mode="overwrite", partition_columns=None)

if __name__ == '__main__':
    root_path = os.path.dirname(os.path.abspath(__file__))
    drews_conf = {'data_path': '/Users/drewdifrancesco/Desktop/data',
                  'root_path': root_path,
                  's3_bucket': None}

    main(None,override_args=drews_conf)