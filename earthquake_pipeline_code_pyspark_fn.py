from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, explode, struct, to_timestamp, from_unixtime, expr
from datetime import datetime
import argparse
import requests
import logging
import  json
from util import Utils
import config as cnf

def earthqueake_pipeline():
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
