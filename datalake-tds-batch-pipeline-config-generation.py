import os
import sys
from datetime import timedelta
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.operators.emr import EmrContainerOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, PythonVirtualenvOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
# from common.operators.my_customglue_operator import GlueJobOperator
from datetime import date, timedelta, datetime, timezone
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.models.param import Param
import boto3
#from time import time,sleep

sys.path.append("/usr/local/airflow/dags/tds/tds-config-generation/")

from config import *

VIRTUAL_CLUSTER_ID = Variable.get('VIRTUAL_CLUSTER_ID')
EXECUTION_ROLE_ARN = Variable.get('EXECUTION_ROLE_ARN')
RELEASE_LABEL = Variable.get('emr_version')
TDS_AWS_CONN_ID = Variable.get('TDS_AWS_CONN_ID')
TDS_SPARK_SUBMIT = Variable.get('TDS_SPARK_SUBMIT')
LOG_GROUP_NAME = "/aws/emr-eks-spark/tds"
table_list = Variable.get('TDS_CONFIG_GENERATION_TABLE_LIST', deserialize_json=True)
s3_log_base_uri = Variable.get('emr_s3_log_base_uri')
s3_log_uri = f"{s3_log_base_uri}/tds/"
configuration_overrides={
                    "monitoringConfiguration": {
                        "s3MonitoringConfiguration": {
                            "logUri": s3_log_uri
                        }
                    },
                }

# Define default_args
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 3,
    "catchup": False,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "is_paused_upon_creation": True,
}


with DAG(
    dag_id       = "datalake-tds-batch-pipeline-config-generation",
    start_date   = datetime(2023, 3, 25),
    schedule     = Variable.get("TDS_CONFIG_GENERATION_SCHEDULE",default_var=None),
    default_args = default_args,
    catchup=False,
    max_active_tasks=50,
    max_active_runs=1,
    is_paused_upon_creation=True,
    description  = 'TDS Config Gen') as dag:

    start = DummyOperator(task_id = "start", dag=dag)
    end   = DummyOperator(task_id = "end", dag=dag)

    distinct_schema = set()
    for t_list in table_list:
        distinct_schema.add(t_list["SCHEMA"])
        distinct_schema_list=list(distinct_schema)
        
    for s in sorted(distinct_schema_list):
        task_group=f"{s}"
        
        with TaskGroup(group_id=task_group) as task_group:
            
            for t_list in table_list:
                TABLE  = t_list["TABLE"]
                SCHEMA = t_list["SCHEMA"]
                if f"{s}" == f"{SCHEMA}":
                    tds_config_generator=EmrContainerOperator(
                        task_id  = f"tds_{TABLE}_config_generator",
                        name = (f"tl-{TABLE}-config-generator")[:64],
                        virtual_cluster_id=VIRTUAL_CLUSTER_ID,
                        execution_role_arn=EXECUTION_ROLE_ARN,
                        release_label=RELEASE_LABEL,        
                        job_driver = {
                        "sparkSubmitJobDriver": {
                        "entryPoint": SCRIPT_LOCATION_CG,
                        "entryPointArguments": ["--config",f"{get_table_config(SCHEMA, TABLE,'CATALOG','PATH')}"],
                        "sparkSubmitParameters":f"{TDS_SPARK_SUBMIT} --conf spark.dynamicAllocation.maxExecutors=1 --conf spark.dynamicAllocation.minExecutors=1 --conf spark.driver.cores=1 --conf spark.driver.memory=1g  --conf spark.executor.memory=1g --conf spark.executor.cores=1"
                        }
                        },
                        configuration_overrides=configuration_overrides,
                        aws_conn_id=TDS_AWS_CONN_ID,        
                        dag=dag
                    )

        start >> task_group >> end

if __name__ == "__main__":
    dag.cli()
