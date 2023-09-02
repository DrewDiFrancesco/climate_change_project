import logging
import os

from pyspark.sql import SparkSession

class SparkManager:
    def __init__(self, args: dict, job_name=None):

        if not isinstance(args, dict):
            args = vars(args)
        self.args = args

        if not job_name:
            job_name = type(self).__name__
        
        self.job_name = job_name
        self.args['running_locally'] = True
        self.spark = None
        self.enginednlines = None
        self.logger = None
        self.log_filepath = os.path.join(self.args['root_path'], "logs")
        if not os.path.exists(self.log_filepath):
            os.mkdir(self.log_filepath)

    def initialize_logger(self, log_filepath):
        log_file = os.path.join(log_filepath, f"{self.job_name}.log")

        logging.basicConfig(
            level=logging.INFO,
            datefmt='%Y-%m-%d %H:%M:%S',
            format='[%(asctime)s] %(module)s: %(levelname)s: %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )

        spark_logger = logging
        handler = logging.FileHandler(log_file)
        handler.setLevel(logging.INFO)

        spark_logger.info(f"Initialized logger to write to log file {log_file}")

        self.logger = spark_logger

    def get_spark(self, jar_file_location=None):
        if self.args['running_locally']:
            spark_config = SparkSession.builder.master("local").appName(self.job_name) \
                .config("spark.master", "local[2]") \
                .config("spark.sql.debug.maxToStringFields", 2000) \
                .config("spark.eventLog.dir", self.log_filepath) \
                .config("spark.eventLog.enabled", "true") \
                .config("spark.driver.memory", self.args['spark_driver_memory']) \
                .config("spark.executor.memory", self.args['spark_driver_memory'])
            if jar_file_location:
                spark_config.config("spark.jars", jar_file_location)
            spark_session = spark_config.getOrCreate()
            spark_session.sparkContext.setLogLevel(self.args['spark_log_level'])
            self.spark = spark_session

        else:
            spark_config = SparkSession.builder.master("yarn").appName(self.job_name)
            if jar_file_location:
                spark_config.config("spark.jars", jar_file_location)
            spark_session = spark_config.getOrCreate()
            spark_session.sparkContext.addPyFile("mode_testing_emr/package.zip")
            spark_session.sparkContext.setLogLevel(self.args['spark_log_level'])
            self.spark = spark_session

        return spark_session