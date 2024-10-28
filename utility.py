################################################################################################
"""
file_name - utility.py
desc - Utility class for handling various tasks related to data processing, including:
       - Fetching data from APIs
       - Writing and reading Parquet files in Google Cloud Storage (GCS)
       - Flattening and processing data
       - Loading processed data into BigQuery tables
       - Logging job execution details into an audit table in BigQuery

start_date - 2024-10-21
"""

import logging
from pyspark.sql.functions import current_timestamp, col, explode, struct, to_timestamp, from_unixtime, expr
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import Row
from datetime import datetime
import json
import requests

class Utils():
    """
    A utility class for data extraction, transformation, loading, and audit logging.
    Contains functions for:
    - Extracting data from an API.
    - Writing and reading Parquet files in Google Cloud Storage (GCS).
    - Flattening, transforming, and cleaning data for processing.
    - Defining schemas for BigQuery tables.
    - Logging job execution details into an audit table in BigQuery for monitoring
    """

    def extractallData(self,api_url):
        """
                Extracts data from an API and returns it as a JSON string.

                Parameters:
                    api_url (str): The API URL to extract data from.

                Returns:
                    str or None: JSON string of extracted data if successful, None otherwise.
        """
        ## by using get method extract the data from api
        response = requests.get(api_url)

        ##Check if the request was successful
        if response.status_code == 200:
            ##convert data into json
            all_data = response.json()  # converts the (api)JSON response data into Python data types (usually a dictionary or a list).
            # print("Extracted Data:", all_data)
            logging.info(f"extract data successfully from {api_url}")
            return json.dumps(all_data)  # Convert the Python dictionary to a JSON string

        else:
            logging.error(f"Failed to retrieve data. Status code: {response.status_code}")
            return None

    def writeParquetDataintoGCS(self,spark, source_data_df, gcs_location):
        """
            Writes a DataFrame as a Parquet file to a specified GCS location.

            Parameters:
                spark (SparkSession): The Spark session.
                source_data_df (DataFrame): The DataFrame to write to GCS.
                gcs_location (str): The GCS path where data will be written.

            Returns:
                None
        """
        source_data_df.coalesce(2).write.mode("overwrite").parquet(gcs_location)
        logging.info(f"Data written to {gcs_location}")

    def readParquetFilefromGCS(self,spark, source_data_loc):
        """
                Reads a Parquet file from GCS and returns it as a DataFrame.

                Parameters:
                    spark (SparkSession): The Spark session.
                    source_data_loc (str): The GCS location of the Parquet file to read.

                Returns:
                    DataFrame: A Spark DataFrame containing the data read from GCS.
        """

        ## read data from gcs
        src_raw_data_df = spark.read.parquet(source_data_loc)
        logging.info(f"read data successfully from {source_data_loc}")
        return src_raw_data_df

    def extractReqDataFlattenApplyTrans(self,raw_data_df):
        """
                Flattens nested JSON data, extracts specific fields, and applies transformations.

                Parameters:
                    raw_data_df (DataFrame): The raw input DataFrame with nested JSON data.

                Returns:
                    DataFrame: A transformed DataFrame with flattened, cleaned, and formatted data.
        """

        # using expoad flatten features
        flattened_feature_df = raw_data_df.select(explode("features").alias("feature"))

        ## Extract properties and coordinates from flattened_feature_df
        properties_coordinated_df = flattened_feature_df.select(
            col("feature.properties.*"),  # Select all fields from properties
            col("feature.geometry.coordinates").alias("coordinates")  # Select coordinates field
        )

        ## conver UNIX timestamps( in milliseconds )to timestamp(Convert milliseconds to seconds and then to readable timestamp)
        ##Generate column “area” - based on existing “place” column
        ## Create a new column 'geometry' with the desired structure and apply transformation
        ## add insert date column
        clean_data_df = (properties_coordinated_df
                         .withColumn('time', to_timestamp(from_unixtime(col('time') / 1000)))
                         .withColumn('updated', to_timestamp(from_unixtime(col('updated') / 1000)))
                         .withColumn('area', expr("substring(place, instr(place, 'of') + 3, length(place))"))
                         .withColumn("geometry",
                                     struct(
                                         col("coordinates")[0].alias("longitude"),
                                         col("coordinates")[1].alias("latitude"),
                                         col("coordinates")[2].alias("depth")
                                     )
                                     )
                         .withColumn('insert_date',current_timestamp())
                         .drop(col("coordinates"))
                         )

        final_clean_data_df = (clean_data_df.select("mag", "place", "time", "updated", "tz", "url",
                                                    "detail", "felt", "cdi", "mmi", "alert", "status",
                                                    "tsunami", "sig", "net", "code", "ids", "sources",
                                                    "types", "nst", "dmin", "rms", "gap", "magType", "type",
                                                    "title", "area", "geometry", "insert_date"
                                                    )
                               )
        # clean_data_df.show()
        # clean_data_df.printSchema()
        logging.info("flatten and transformation done")
        return final_clean_data_df

    def bqSchema(self):
        """
                 Defines the schema for BigQuery.

                Returns:
                    dict: A dictionary representing the schema for BigQuery.
        """

        # Define the schema with mode
        bq_schema = {
            "fields": [
                {"name": "mag", "type": "FLOAT64", "mode": "NULLABLE"},
                {"name": "place", "type": "STRING", "mode": "NULLABLE"},
                {"name": "time", "type": "TIMESTAMP", "mode": "NULLABLE"},
                {"name": "updated", "type": "TIMESTAMP", "mode": "NULLABLE"},
                {"name": "tz", "type": "STRING", "mode": "NULLABLE"},
                {"name": "url", "type": "STRING", "mode": "NULLABLE"},
                {"name": "detail", "type": "STRING", "mode": "NULLABLE"},
                {"name": "felt", "type": "INT64", "mode": "NULLABLE"},
                {"name": "cdi", "type": "FLOAT64", "mode": "NULLABLE"},
                {"name": "mmi", "type": "FLOAT64", "mode": "NULLABLE"},
                {"name": "alert", "type": "STRING", "mode": "NULLABLE"},
                {"name": "status", "type": "STRING", "mode": "NULLABLE"},
                {"name": "tsunami", "type": "INT64", "mode": "NULLABLE"},
                {"name": "sig", "type": "INT64", "mode": "NULLABLE"},
                {"name": "net", "type": "STRING", "mode": "NULLABLE"},
                {"name": "code", "type": "STRING", "mode": "NULLABLE"},
                {"name": "ids", "type": "STRING", "mode": "NULLABLE"},
                {"name": "sources", "type": "STRING", "mode": "NULLABLE"},
                {"name": "types", "type": "STRING", "mode": "NULLABLE"},
                {"name": "nst", "type": "INT64", "mode": "NULLABLE"},
                {"name": "dmin", "type": "FLOAT64", "mode": "NULLABLE"},
                {"name": "rms", "type": "FLOAT64", "mode": "NULLABLE"},
                {"name": "gap", "type": "FLOAT64", "mode": "NULLABLE"},
                {"name": "magType", "type": "STRING", "mode": "NULLABLE"},
                {"name": "type", "type": "STRING", "mode": "NULLABLE"},
                {"name": "title", "type": "STRING", "mode": "NULLABLE"},
                {"name": "area", "type": "STRING", "mode": "NULLABLE"},
                {
                    "name": "geometry", "type": "RECORD", "mode": "REQUIRED", "fields": [
                    {"name": "longitude", "type": "FLOAT64", "mode": "NULLABLE"},
                    {"name": "latitude", "type": "FLOAT64", "mode": "NULLABLE"},
                    {"name": "depth", "type": "FLOAT64", "mode": "NULLABLE"}
                ]
                },
                {"name": "insert_date", "type": "TIMESTAMP", "mode": "REQUIRED"}
            ]}

        return bq_schema

    def writeDataintoBigquery(self,output_db, data_df, bq_schema=None):
        """
                Writes data from a DataFrame to a specified BigQuery table.

                Parameters:
                    output_db (str): BigQuery table path in format 'project_id.dataset_id.table_id'.
                    data_df (DataFrame): The DataFrame containing the data to be written.
                    bq_schema (dict, optional): BigQuery schema. If None, uses self.bqSchema().

                Returns:
                    None
                """

        if bq_schema is None:
            # call function bqSchema to get bq schema
            bq_schema = self.bqSchema()
        data_df.write.format('bigquery').option("table", output_db) \
            .option('schema', bq_schema) \
            .option("createDisposition", "CREATE_IF_NEEDED") \
            .option("writeDisposition", "WRITE_APPEND") \
            .mode('append')\
            .save()
        logging.info(f"Data load successfully to {output_db}")

    def createDFforAuditTbl(self,spark_1, job_id, pipeline_name, function_name, start_time, end_time, status,error_msg,
                            process_record=0):

        """
                Creates a DataFrame for audit logs with job execution details.

                Parameters:
                    spark_1 (SparkSession): The Spark session to create a DataFrame.
                    job_id (str): Unique identifier for the job.
                    pipeline_name (str): Name of the data pipeline.
                    function_name (str): Name of the function executing the job.
                    start_time (str): Start time of the job execution.
                    end_time (str): End time of the job execution.
                    status (str): Status of the job execution (e.g., SUCCESS, FAILURE).
                    error_msg(str):  error message (string) for failed jobs.
                    process_record (int, optional): Number of records processed. Defaults to 0.

                Returns:
                    DataFrame: A DataFrame containing the audit log entry.
        """

        # Create audit entries using Row
        audit_entry = [Row(job_id=job_id,
                           pipeline_name=pipeline_name,
                           function_name=function_name,
                           start_time=start_time,
                           end_time=end_time,
                           status=status,
                           error_msg=error_msg,
                           process_record=process_record)]

        schema = StructType([
            StructField('job_id', StringType(), True),
            StructField('pipeline_name', StringType(), True),
            StructField('function_name', StringType(), True),
            StructField('start_time', StringType(), True),
            StructField('end_time', StringType(), True),
            StructField('status', StringType(), True),
            StructField('error_msg', StringType(), True),
            StructField('process_record', IntegerType(), True),

        ])

        # Create DataFrame with the provided schema
        audit_df = spark_1.createDataFrame(audit_entry, schema)

        return audit_df

    ### defind function for create audit tbl schema
    def auditTblSchema(self):
        """
                Defines the schema for the audit table in BigQuery.

                Returns:
                    list: A list of dictionaries defining the schema for the audit table.
        """
        audit_table_schema = [
            {"name": "job_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "pipeline_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "function_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "start_time", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "end_time", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "status", "type": "STRING", "mode": "NULLABLE"},
            {"name": "error_msg", "type": "STRING", "mode": "NULLABLE"},
            {"name": "process_record", "type": "INTEGER", "mode": "NULLABLE"}
        ]
        return audit_table_schema


    def log_audit(self, spark, job_id, pipeline_name, function_name, start_time, end_time, status, process_record,audit_output_db,error_msg):
        """Logs audit information to BigQuery.
                Parameters:
                - spark: SparkSession used for DataFrame operations.
                - job_id: Unique identifier for the job.
                - pipeline_name: Name of the pipeline.
                - function_name: Name of the function.
                - start_time: Job start time (string).
                - end_time: Job end time (string).
                - status: Job status (e.g., "SUCCESS", "FAILED").
                - process_record: Number of records processed.
                - audit_output_db: BigQuery dataset/table for logging.
                - error_msg:  error message (string) for failed jobs.

                This function creates an audit DataFrame and writes it to BigQuery for tracking job performance.

        """
        audit_df = self.createDFforAuditTbl(spark, job_id, pipeline_name, function_name, start_time, end_time, status,error_msg,process_record)
        audit_table_schema = self.auditTblSchema()
        self.writeDataintoBigquery(audit_output_db, audit_df, audit_table_schema)









