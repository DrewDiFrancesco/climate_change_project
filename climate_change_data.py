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
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window




def main(spark=None, override_args=None):

    """
    Perform data processing, ETL (Extract, Transform, Load) operations, and write results to files.

    This function is the main entry point for data processing and ETL operations. It handles various tasks,
    including updating configuration parameters, initializing Spark if necessary, processing Excel data,
    performing data transformations, and writing the results to files. The behavior can be customized
    using override arguments.

    Args:
        spark (SparkSession, optional): An existing SparkSession instance. If not provided, a new one will be created.
        override_args (dict, optional): A dictionary of override arguments to modify the default behavior.

    Note:
        - Override arguments can be used to modify the behavior of the data processing and ETL pipeline.
        - If `spark` is not provided, a new SparkSession instance will be created.
        - Configuration parameters are updated based on default arguments and override settings.
        - Data is processed, transformed, and written to files based on the specified configuration.

    """

    print(f"HERE is the override args: {override_args}")

    if override_args:
        default_args.update(override_args)

    print(f"HERE is the new default args: {default_args}")
    config_manager = config.Config(default_args)
    
    running_locally = config_manager.args['running_locally']

    if spark is None:
        print(f"Getting Spark")
        spark = SparkManager(config_manager.args).get_spark()
        print("Done getting spark")

    if config_manager.args['s3_bucket'] != '':
        print(f"Updating data_path because s3_bucket is not an empty string...")
        config_manager.args['data_path'] = config_manager.args['s3_bucket']+'/data'
    
    data_path = config_manager.args['data_path']
    non_agg_partition_col = ["Year", "Region"]

    # Specify the path to your Excel file
    print(f"Reading in initial data from {data_path}")
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
    
    s_income_level_df = spark.createDataFrame(income_level_df, schema)
    s_region_df = spark.createDataFrame(region_df, schema)
    s_non_aggregated_df = spark.createDataFrame(non_aggregated_df, schema)

    s_income_level_df = s_income_level_df.filter(col("Value").cast("double").isNotNull())
    s_income_level_df = s_income_level_df.drop('Decimals')
    s_income_level_df = s_income_level_df.drop('SCALE')

    s_region_df = s_region_df.filter(col("Value").cast("double").isNotNull())
    s_region_df = s_region_df.drop('Decimals')
    s_region_df = s_region_df.drop('SCALE')

    s_non_aggregated_df = s_non_aggregated_df\
        .filter(col("Value").cast("double").isNotNull())\
        .filter('Value is not null')\
        .filter(col('Value') != 'NaN')
    s_non_aggregated_df = s_non_aggregated_df.drop('Decimals')
    s_non_aggregated_df = s_non_aggregated_df.drop('SCALE')

    metric_high_bad_list = ['Agricultural land under irrigation (% of total ag. land)',
                            'Annual freshwater withdrawals (% of internal resources)',
                            'CO2 emissions per capita (metric tons)',
                            'CO2 emissions per units of GDP (kg/$1,000 of 2005 PPP $)',
                            'CO2 emissions, total (KtCO2)',
                            'Child malnutrition, underweight (% of under age 5)',
                            'Droughts, floods, extreme temps (% pop. avg. 1990-2009)',
                            'Ease of doing business (ranking 1-183; 1=best)',
                            'Energy use per capita (kilograms of oil equivalent)',
                            'Energy use per units of GDP (kg oil eq./$1,000 of 2005 PPP $)',
                            'Foreign direct investment, net inflows (% of GDP)', 'GDP ($)',
                            'Land area below 5m (% of land area)',
                            'Malaria incidence rate (per 100,000 people)',
                            'Methane (CH4) emissions, total (KtCO2e)',
                            'Nitrous oxide (N2O) emissions, total (KtCO2e)',
                            'Other GHG emissions, total (KtCO2e)',
                            'Population below 5m (% of total)',
                            'Population living below $1.25 a day (% of total)',
                            'Projected annual precipitation change (2045-2065, mm)',
                            'Projected annual temperature change (2045-2065, Celsius)',
                            'Projected change in annual cool days/cold nights',
                            'Projected change in annual hot days/warm nights',
                            'Under-five mortality rate (per 1,000)',
    ]
    columns_to_remove = ['Annex-I emissions reduction target',
                         'Average daily min/max temperature (1961-1990, Celsius)',
                         'Latest UNFCCC national communication',
                         'NAMA submission',
                         'NAPA submission',
                         'Population in urban agglomerations >1million (%)',
                         'Ratio of girls to boys in primary & secondary school (%)',
                         'Urban population', 
                         'Urban population growth (annual %)', 'Capital city',
                         'Region', 'Income group', 'Lending category',
                         'Series code'
    ]

    map_data = s_non_aggregated_df\
        .groupBy(['Series name','Country name'])\
        .agg(avg('Value').alias("Value"))
    map_data = map_data.withColumn('Value', round(col('Value'),1))

    def getPercentile(df,metric_high_bad_list,partition_columns):
        df = df.withColumn("high_value_is_bad", 
        when(col('Series name').isin(metric_high_bad_list), True)
        .otherwise(False))
        window_spec = Window.partitionBy(partition_columns).orderBy("Value")
        df = df.withColumn("value_percentile", percent_rank().over(window_spec))
        df = df.withColumn("value_percentile", 
            when(df["high_value_is_bad"], 1 - df["value_percentile"])
            .otherwise(df["value_percentile"]))
        df = df.drop("high_value_is_bad")
        df = df.withColumn('value_percentile', round(100*col('value_percentile'),1))
        return df
    

    map_data = getPercentile(map_data,metric_high_bad_list,['Series name'])
    s_non_aggregated_df = getPercentile(s_non_aggregated_df,metric_high_bad_list,
        ['Year','Series name'])

    # Drop unnecessary columns
    # map_data = map_data.drop("high_value_is_bad", "Year", "value_list")

    for output_type in ['parquet','csv']:
        final_data_path = data_path
        if output_type == 'csv':
            final_data_path = os.path.join(data_path, 'power_BI_data')
        # etl_helpers.write_data(s_income_level_df, path_to_files=final_data_path, file_name="agg_income_level", 
        #                        mode="overwrite", output_type = output_type)
        # etl_helpers.write_data(s_region_df, path_to_files=final_data_path, file_name="agg_region", mode="overwrite", 
        #                         output_type = output_type)
        # etl_helpers.write_data(s_non_aggregated_df, path_to_files=final_data_path, file_name="climate_change_data", 
        #                        mode="overwrite", partition_columns=non_agg_partition_col, output_type = output_type)
        etl_helpers.write_data(map_data, path_to_files=final_data_path, file_name="map_data", 
                               mode="overwrite", output_type = output_type)

if __name__ == '__main__':
    root_path = os.path.dirname(os.path.abspath(__file__))
    drews_conf = {'data_path': '/Users/drewdifrancesco/Desktop/data',
                  'root_path': root_path,
                  's3_bucket': ''}

    main(None,override_args=drews_conf)
    # main(None,override_args={})