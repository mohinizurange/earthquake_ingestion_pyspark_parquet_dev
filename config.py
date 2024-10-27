from datetime import datetime


## paths
cur_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
eq_landing_gcs_loc = f"gs://earthquake-prj/landing/{cur_timestamp}/"
eq_silver_gcs_loc =f'gs://earthquake-prj/silver/{cur_timestamp}/'
project_id = 'bwt-project-433109'
temp_dataproc_bucket = "earthquake-dp_temp_bk"
eq_bigquery_tbl_loc ='bwt-project-433109.earthquake_db.earthquake_data'
eq_audit_tbl_loc = 'bwt-project-433109.earthquake_db.earthquake_audit_tbl'

# api_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"
