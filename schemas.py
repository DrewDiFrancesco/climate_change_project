from pyspark.sql.types import *

# Define the schema for the DataFrame
climate_change_data_spark = StructType([
    StructField("Country code", StringType(), True),
    StructField("Country name", StringType(), True),
    StructField("Series code", StringType(), True),
    StructField("Series name", StringType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Value", DoubleType(), True),
    StructField("Capital city", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("Income group", StringType(), True),
    StructField("Lending category", StringType(), True),
    StructField("Topic", StringType(), True),
    StructField("Definition", StringType(), True),
    StructField("value_percentile", FloatType(), True)
])

climate_change_data_pandas = {
    "Country code": str,
    "Country name": str,
    "Series code": str,
    "Series name": str,
    "Year": int,
    "Value": float,
    "Capital city": str,
    "Region": str,
    "Income group": str,
    "Lending category": str,
    "Topic": str,
    "Definition": str,
    "value_percentile": float
}

agg_region_spark = StructType([
    StructField("Country code", StringType(), True),
    StructField("Country name", StringType(), True),
    StructField("Series code", StringType(), True),
    StructField("Series name", StringType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Value", DoubleType(), True),
    StructField("Capital city", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("Income group", StringType(), True),
    StructField("Lending category", StringType(), True),
    StructField("Topic", StringType(), True),
    StructField("Definition", StringType(), True)
])

agg_region_pandas = {
    "Country code": str,
    "Country name": str,
    "Series code": str,
    "Series name": str,
    "Year": int,
    "Value": float,
    "Capital city": str,
    "Region": str,
    "Income group": str,
    "Lending category": str,
    "Topic": str,
    "Definition": str
}

climate_change_data_pandas = {
    "Country code": str,
    "Country name": str,
    "Series code": str,
    "Series name": str,
    "Year": int,
    "Value": float,
    "Capital city": str,
    "Region": str,
    "Income group": str,
    "Lending category": str,
    "Topic": str,
    "Definition": str
}

agg_income_level_spark = StructType([
    StructField("Country code", StringType(), True),
    StructField("Country name", StringType(), True),
    StructField("Series code", StringType(), True),
    StructField("Series name", StringType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Value", DoubleType(), True),
    StructField("Capital city", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("Income group", StringType(), True),
    StructField("Lending category", StringType(), True),
    StructField("Topic", StringType(), True),
    StructField("Definition", StringType(), True)
])

agg_income_level_pandas = {
    "Country code": str,
    "Country name": str,
    "Series code": str,
    "Series name": str,
    "Year": int,
    "Value": float,
    "Capital city": str,
    "Region": str,
    "Income group": str,
    "Lending category": str,
    "Topic": str,
    "Definition": str
}

map_data_spark = StructType([
    StructField("Country name", StringType(), True),
    StructField("Series name", StringType(), True),
    StructField("Value", FloatType(), True),
    StructField("value_percentile", FloatType(), True)
])

map_data_pandas = {
    "Country name": str,
    "Series name": str,
    "Value": float,    
    "value_percentile": float
}