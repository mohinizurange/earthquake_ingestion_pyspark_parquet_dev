from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, explode, struct, to_timestamp, from_unixtime, expr
from datetime import datetime
import argparse
import requests
import logging
import  json
from util import Utils
import config as cnf



if __name__=='__main__':
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Initialize Spark session and configs
    spark = SparkSession.builder.master("local[*]").appName("extract_the_data_from_API").getOrCreate()
    spark.conf.set("temporaryGcsBucket", cnf.temp_dataproc_bucket)

    ## create Util obj
    util_obj = Utils()

    # Parse command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-api_url', '--api_url', required=True, help='API URL required')
    parser.add_argument('-pipeline_nm', '--pipeline_nm', required=True, help='Pipeline name')
    args = parser.parse_args()

    api_url = args.api_url
    pipeline_name = args.pipeline_nm

    ##1. call func extractallData for extract data from api
    source_data = util_obj.extractallData(api_url)
    ##create df
    extracted_data_df = spark.read.json(spark.sparkContext.parallelize([source_data]))

    ##2. call function writeParquetDataintoGCS to write data into landing location
    util_obj.writeParquetDataintoGCS(spark, extracted_data_df,cnf.eq_landing_gcs_loc)

    ##3. read parquet file from landing loc
    raw_data =util_obj.readParquetFilefromGCS(spark, cnf.eq_landing_gcs_loc)

    ##4. extract the required data and flatten it also apply transformation
    clean_data = util_obj.extractReqDataFlattenApplyTrans(raw_data)


    ##5. write clean data into gcs in parquet format (silver location)
    util_obj.writeParquetDataintoGCS(spark, clean_data, cnf.eq_silver_gcs_loc)

    ##6. write clean data into bigquery table
    util_obj.writeDataintoBigquery(cnf.eq_bigquery_tbl_loc, clean_data)


