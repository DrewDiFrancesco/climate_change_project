default_args = {
    's3_bucket': "s3://climatechangeprojectdrewdifrancesco",
    'spark_driver_memory': '3g',
    'spark_log_level': 'ERROR'
}

if default_args['s3_bucket'] != '':
    default_args['data_path'] = default_args['s3_bucket']+'/data'