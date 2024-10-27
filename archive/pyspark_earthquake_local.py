from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, explode, struct, to_timestamp, from_unixtime, expr
from datetime import datetime
import argparse
import requests
import logging
import  json


def extractallData( api_url):

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

def writeParquetDataintoGCS(spark,source_data_df, gcs_location):

    source_data_df.coalesce(2).write.mode("overwrite").parquet(gcs_location)
    logging.info(f"Data written to {gcs_location}")


def readParquetFilefromGCS(spark,source_data_loc):
    # Read back the Parquet data from GCS
    src_raw_data_df = spark.read.parquet(source_data_loc)

    return src_raw_data_df

def extractRequiredDataAndFlatten(raw_data_df):

    # using expoad flatten features
    flattened_feature_df = raw_data.select(explode("features").alias("feature"))
    # flattened_feature_df.show()
    # flattened_feature_df.printSchema()

    ## Extract properties and coordinates from flattened_feature_df
    properties_coordinated_df = flattened_feature_df.select(
        col("feature.properties.*"),  # Select all fields from properties
        col("feature.geometry.coordinates").alias("coordinates")  # Select coordinates field
    )
    # required_data_df.show()
    # required_data_df.printSchema()

    ## conver UNIX timestamps( in milliseconds )to timestamp(Convert milliseconds to seconds and then to readable timestamp)
    ##Generate column “area” - based on existing “place” column
    ## Create a new column 'geometry' with the desired structure and apply transformation
    ## add insert date column
    clean_data_df = (properties_coordinated_df
                                            .withColumn('time', to_timestamp(from_unixtime(col('time') / 1000)))
                                            .withColumn('updated', to_timestamp(from_unixtime(col('updated') / 1000)))
                                            .withColumn('area', expr("substring(place, instr(place, 'of') + 3, length(place))"))
                                            .withColumn( "geometry",
                                                        struct(
                                                            col("coordinates")[0].alias("longitude"),
                                                            col("coordinates")[1].alias("latitude"),
                                                            col("coordinates")[2].alias("depth")
                                                              )
                                                     )
                                            .withColumn('insert_date',current_timestamp())  ##  we can also use  lit(insert_date )= datetime.now().strftime('%Y%m%d %H%M%S')

                     .drop(col("coordinates"))
                )

    final_clean_data_df = ( clean_data_df.select("mag", "place", "time", "updated", "tz", "url",
                                                      "detail", "felt", "cdi", "mmi", "alert", "status",
                                                      "tsunami", "sig", "net", "code", "ids", "sources",
                                                      "types", "nst", "dmin", "rms", "gap", "magType", "type",
                                                      "title", "area","geometry","insert_date"
                                                      )
                      )
    # clean_data_df.show()
    # clean_data_df.printSchema()
    return final_clean_data_df

def writeDataBigquery(self,output_db, data_df,bq_schema=None):
    """
            Writes data from a DataFrame to a specified BigQuery table.

            Parameters:
                output_db (str): The target BigQuery table in the format 'project_id.dataset_id.table_id'.
                data_df (DataFrame): The DataFrame containing the data to be written.
                bq_schema (list, optional): The schema for the BigQuery table. If None, the schema will be fetched using the bqSchema method.

            Returns:
                None
    """
    if bq_schema is None:
        ##call function bqSchema to get bq schema
        bq_schema = self.bqSchema()
    data_df.write.format('bigquery').option("table", output_db) \
        .option('schema',bq_schema)\
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("writeDisposition", "WRITE_APPEND") \
        .mode('append') \
        .save()



if __name__=='__main__':
    spark = SparkSession.builder.master("local[*]").appName("extract_the_data_from_API").getOrCreate()
    api_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"

    source_data = extractallData(api_url)


    extracted_data_df = spark.read.json(spark.sparkContext.parallelize([source_data]))
    # df.printSchema()

    cur_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    ## landing location
    eq_landing_gcs_loc = f"D:/Mohini Data Science/parquet_testing/landing/{cur_timestamp}/"
    # eq_landing_gcs_loc =r"D:\Mohini Data Science\parquet_testing\landing\20241027_170041"
    writeParquetDataintoGCS(spark, extracted_data_df, eq_landing_gcs_loc)

    ## read data from landing loc
    raw_data =readParquetFilefromGCS(spark, eq_landing_gcs_loc)
    # df.show(truncate=False)
    # df.printSchema()

    ## extract the required data and flatten it
    clean_data = extractRequiredDataAndFlatten(raw_data)
    # clean_data.show()
    # clean_data.printSchema()

    ## write clean data into gcs silver location
    eq_silver_gcs_loc =f'D:/Mohini Data Science/parquet_testing/silver/{cur_timestamp}/'
    writeParquetDataintoGCS(spark, clean_data, eq_silver_gcs_loc)

    ## write clean data into bigquery table


