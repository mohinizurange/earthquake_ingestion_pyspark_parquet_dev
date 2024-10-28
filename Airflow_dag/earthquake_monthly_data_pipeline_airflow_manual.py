from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocDeleteClusterOperator
)


from datetime import datetime,timedelta


# Define default arguments
default_args = {'owner':'M_Airflow',
               'retrives':2,
               'depend_on_past':'False',
               'retry_delay':timedelta(minutes=1),
               'start_date':datetime(2024,10,28),
               }
# Define DAG
with DAG(
    'Earthquake_monthly_dataload_manual',
    default_args=default_args,
    schedule_interval=None,  # manual run
    catchup=False,
) as dag:
    # Step 1: Create Dataproc Cluster
    create_cluster = BashOperator(
        task_id='create_cluster',
        bash_command="""
        gcloud dataproc clusters create dataproc-cluster\
                --bucket earthquake-dp_temp_bk \
                --enable-component-gateway \
                --region us-central1 \
                --zone us-central1-a \
                --master-machine-type e2-standard-2 \
                --master-boot-disk-size 100 \
                --num-workers 2 \
                --worker-machine-type e2-standard-2 \
                --worker-boot-disk-size 100 \
                --image-version 2.0-debian10 \
                --scopes https://www.googleapis.com/auth/cloud-platform  \
                --tags pyspark \
                --project bwt-project-433109 \
                --initialization-actions gs://goog-dataproc-initialization-actions-us-central1/connectors/connectors.sh \
                --metadata bigquery-connector-version=1.2.0 \
                --metadata spark-bigquery-connector-version=0.21.0
        """
    )

    # Step 2: Submit Spark Job

    submit_spark_job=BashOperator(
        task_id='submit_spark_job',
        bash_command=""" gcloud dataproc jobs submit pyspark  \
                        gs://earthquake-prj/earthquake_pipeline_code_pyspark_fn.py \
                        --py-files gs://earthquake-prj/utility.py,gs://earthquake-prj/config.py \
                        --cluster=dataproc-cluster \
                        --region=us-central1 \
                        -- \
                        --api_url="https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson" \
                        --pipeline_nm='Monthly' """

                                   )

    # # # Step 3:delete cluster

    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        project_id='bwt-project-433109',
        cluster_name='dataproc-cluster',
        region='us-central1',
        trigger_rule='all_done',  ###all_success (default) only for task it didnt check the job is completed or not
        gcp_conn_id='gcp_connection',
    )

    # Define Task Dependencies
    create_cluster >> submit_spark_job >>delete_cluster


