################################################################################################
"""
file_name - extract_earthquake_data_pipeline.py
desc - Main script for executing the Earthquake Data Extraction and Transformation pipeline.
       This script performs the following operations:
       - Configures logging for tracking pipeline execution.
       - Initializes a Spark session with necessary configurations.
       - Creates an instance of the Utils class
       - Parses command-line arguments for API URL and pipeline name.
       - Executes a series of functions to:
         1. Extract data from the specified API.
         2. Write the extracted data to a landing location in GCS in Parquet format.
         3. Read the Parquet file from GCS.
         4. Extract and transform the required data by flattening it.
         5. Write the cleaned data to a silver location in GCS in Parquet format.
         6. Load the cleaned data into a specified BigQuery table.

start_date - 2024-10-21
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, explode, struct, to_timestamp, from_unixtime, expr
from datetime import datetime
import argparse
import requests
import logging
import  json
from utility import Utils
import config as cnf

def earthqueake_pipeline():
    def earthqueake_pipeline():
        """
        Executes an ETL pipeline for earthquake data extraction, transformation,
        and loading using Apache Spark.

        Steps:
        1. Extracts data from a specified API into a Spark DataFrame.
        2. Writes the data to GCS in Parquet format.
        3. Reads the Parquet file from GCS.
        4. Flattens and transforms the data.
        5. Writes cleaned data to GCS (silver location).
        6. Loads the cleaned data into a BigQuery table.

        Command-line arguments:
        - api_url: API URL for data extraction.
        - pipeline_nm: Name of the pipeline.

        Logs the start and end times, status, and processed records for each step.
        Raises exceptions on failure.
        """
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Initialize Spark session and configs
    spark = SparkSession.builder.master("local[*]").appName("extract_the_data_from_API").getOrCreate()
    spark.conf.set("temporaryGcsBucket", cnf.temp_dataproc_bucket)

    ## create Util classs obj
    util_obj = Utils()

    # Parse command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-api_url', '--api_url', required=True, help='API URL required')
    parser.add_argument('-pipeline_nm', '--pipeline_nm', required=True, help='Pipeline name')
    args = parser.parse_args()

    api_url = args.api_url
    pipeline_name = args.pipeline_nm

    job_id = cnf.cur_timestamp
    # Function 1: Extract data from API
    try:
        function_name = "1_extractallData"
        start_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        ### 1 : function call #####
        source_data = util_obj.extractallData(api_url)
        extracted_data_df = spark.read.json(spark.sparkContext.parallelize([source_data]))

        end_time, status = datetime.now().strftime('%Y%m%d_%H%M%S'), "successful"
        process_record = extracted_data_df.count()
    except Exception as e:
        end_time, status = datetime.now().strftime('%Y%m%d_%H%M%S'), "fail"
        process_record = 0
        error_msg = e
        logging.error(f"Error in {function_name}: {e}")
    util_obj.log_audit(spark, job_id, pipeline_name, function_name, start_time, end_time, status, process_record,cnf.eq_audit_tbl_loc,error_msg=None)

    # Function 2: Write Parquet Data to GCS (landing location)
    try:
        function_name = "2_writeParquetDataintoGCS"
        start_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        ### 2 : function call #####
        util_obj.writeParquetDataintoGCS(spark, extracted_data_df, cnf.eq_landing_gcs_loc)
        end_time, status = datetime.now().strftime('%Y%m%d_%H%M%S'), "successful"
        process_record = extracted_data_df.count()
    except Exception as e:
        end_time, status = datetime.now().strftime('%Y%m%d_%H%M%S'), "fail"
        process_record = 0
        error_msg=e
        logging.error(f"Error in {function_name}: {e}")
    util_obj.log_audit(spark, job_id, pipeline_name, function_name, start_time, end_time, status, process_record,cnf.eq_audit_tbl_loc,error_msg=None)

    # Function 3: Read parquest file from GCS (landing location)
    try:
        function_name = "3_readParquetFilefromGCS"
        start_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        ### 3 : function call #####
        raw_data = util_obj.readParquetFilefromGCS(spark, cnf.eq_landing_gcs_loc)
        end_time, status = datetime.now().strftime('%Y%m%d_%H%M%S'), "successful"
        process_record = raw_data.count()
    except Exception as e:
        end_time, status = datetime.now().strftime('%Y%m%d_%H%M%S'), "fail"
        process_record = 0
        error_msg=e
        logging.error(f"Error in {function_name}: {e}")
    util_obj.log_audit(spark, job_id, pipeline_name, function_name, start_time, end_time, status, process_record,cnf.eq_audit_tbl_loc,error_msg=None)

    # Function 4: extract the required data and flatten it as well as apply transformation
    try:
        function_name = "4_extractReqDataFlattenApplyTrans"
        start_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        ### 4 : function call #####
        clean_data = util_obj.extractReqDataFlattenApplyTrans(raw_data)
        end_time, status = datetime.now().strftime('%Y%m%d_%H%M%S'), "successful"
        process_record = clean_data.count()
    except Exception as e:
        end_time, status = datetime.now().strftime('%Y%m%d_%H%M%S'), "fail"
        process_record = 0
        error_msg=e
        logging.error(f"Error in {function_name}: {e}")
    util_obj.log_audit(spark, job_id, pipeline_name, function_name, start_time, end_time, status, process_record,cnf.eq_audit_tbl_loc,error_msg=None)

    # Function 5: write clean data into gcs in parquet format (silver location)
    try:
        function_name = "5_writeParquetDataintoGCS"
        start_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        ### 5 : function call #####
        util_obj.writeParquetDataintoGCS(spark, clean_data, cnf.eq_silver_gcs_loc)
        end_time, status = datetime.now().strftime('%Y%m%d_%H%M%S'), "successful"
        process_record = clean_data.count()
    except Exception as e:
        end_time, status = datetime.now().strftime('%Y%m%d_%H%M%S'), "fail"
        process_record = 0
        error_msg = e
        logging.error(f"Error in {function_name}: {e}")
    util_obj.log_audit(spark, job_id, pipeline_name, function_name, start_time, end_time, status, process_record,cnf.eq_audit_tbl_loc, error_msg=None)

    # Function 6: write clean data into bigquery table
    try:
        function_name = "6_writeDataintoBigquery"
        start_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        ### 6: function call #####
        util_obj.writeDataintoBigquery(cnf.eq_bigquery_tbl_loc, clean_data)
        end_time, status = datetime.now().strftime('%Y%m%d_%H%M%S'), "successful"
        process_record = clean_data.count()
    except Exception as e:
        end_time, status = datetime.now().strftime('%Y%m%d_%H%M%S'), "fail"
        process_record = 0
        error_msg = e
        logging.error(f"Error in {function_name}: {e}")
    util_obj.log_audit(spark, job_id, pipeline_name, function_name, start_time, end_time, status, process_record,cnf.eq_audit_tbl_loc, error_msg=None)


if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    earthqueake_pipeline()
