import sys
import os

sys.path.append("/usr/local/airflow/dags/tds/enty-batch")

#from variables import *
from airflow.models import Variable

GLUE_ROLE_ARN        = Variable.get("GLUE_ROLE_ARN")
ENDPOINT_URL         = Variable.get("ENDPOINT_URL")
TDS_JOB_FILE_BUCKET_NAME = Variable.get("TDS_JOB_FILE_BUCKET_NAME")
TDS_TARGET_BUCKET_NAME   = Variable.get("TDS_TARGET_BUCKET_NAME")
TARGET_BUCKET_NAME = Variable.get("TARGET_BUCKET_NAME")
WAREHOUSE_PATH_DIR   = f"s3://{TDS_TARGET_BUCKET_NAME}/athena_warehouse/"

SCRIPT_LOCATION_CG = f"s3://{TDS_JOB_FILE_BUCKET_NAME}/tds/scripts/emr_tds_template_file.py"
SCRIPT_LOCATION_TL = f"s3://{TDS_JOB_FILE_BUCKET_NAME}/tds/scripts/emr_tds_template_transform.py"
SCRIPT_LOCATION_FL = f"s3://{TDS_JOB_FILE_BUCKET_NAME}/tds/scripts/emr_tds_template_merge.py"
SCRIPT_LOCATION_DQ = f"s3://{TDS_JOB_FILE_BUCKET_NAME}/tds/scripts/emr_tds_count_comparision.py"
SCRIPT_LOCATION_COMP = f"s3://{TDS_JOB_FILE_BUCKET_NAME}/tds/scripts/emr_tds_compaction.py"

# def get_job_args():
def get_table_config(src_schema, src_table, tgt_schema, tgt_table):
    table_details = {
        "CATALOG_NAME" : "glue_catalog",
        "WAREHOUSE_PATH" : f"{WAREHOUSE_PATH_DIR}",
        "source_schema"  : src_schema,
        "source_table"   : src_table,
        "target_schema"  : tgt_schema,
        "target_table"  : tgt_table,
        "source_bucket_name" : TDS_JOB_FILE_BUCKET_NAME,
        "target_tds_bucket_name" : TDS_TARGET_BUCKET_NAME,
        "rl_target_bucket_name" : TARGET_BUCKET_NAME,        
        "config_csv_file_path" : f"tds/mappings/enty/{tgt_table}_mapping.csv",
        "config_jsn_file_path" : f"tds/config/enty/{tgt_table}_config.json",
        "multiple_sources" : "N",
        "batch_flag" : "Y"
    }

    return table_details

table_list = [
    {
        "src_schema" : "rl_enty_def",
        "src_table" : "tb_ventypaymethod",
        "tgt_schema" : "tl_enty_def",
        "tgt_table" : "tb_v_entypaymethod_hist",
        "resource_size" : "small"
    },
    {
        "src_schema" : "rl_enty_def",
        "src_table" : "tb_gogreenindicator",
        "tgt_schema" : "tl_enty_def",
        "tgt_table" : "tb_gogreenindicator_hist",
        "resource_size" : "small"
    },
    {
        "src_schema" : "rl_enty_def",
        "src_table" : "tb_ventygrade",
        "tgt_schema" : "tl_enty_def",
        "tgt_table" : "tb_v_entygrade_hist",
        "resource_size" : "medium"
    }    
]