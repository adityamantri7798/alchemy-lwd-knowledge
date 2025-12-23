import sys
import os

sys.path.append("/usr/local/airflow/dags/tds/tds-config-generation/")

#from variables import *
from airflow.models import Variable

TDS_JOB_FILE_BUCKET_NAME = Variable.get("TDS_JOB_FILE_BUCKET_NAME")
WAREHOUSE_PATH_DIR   = f"s3://{TDS_JOB_FILE_BUCKET_NAME}/athena_warehouse/"
SCRIPT_LOCATION_CG = f"s3://{TDS_JOB_FILE_BUCKET_NAME}/tds/scripts/emr_tds_template_file.py"

# def get_job_args():
def get_table_config(SCHEMA, TABLE,CATALOG,PATH):
    table_details = {
        "CATALOG_NAME" : "glue_catalog",
        "WAREHOUSE_PATH" : f"{WAREHOUSE_PATH_DIR}",
        "SCHEMA"  : SCHEMA,
        "TABLE"   : TABLE,
        "source_bucket_name" : TDS_JOB_FILE_BUCKET_NAME,
        "config_csv_file_path" : f"tds/mappings/{SCHEMA.split('_')[1]}/{TABLE}_mapping.csv",
        "config_jsn_file_path" : f"tds/config/{SCHEMA.split('_')[1]}/{TABLE}_config.json",        
    }

    return table_details