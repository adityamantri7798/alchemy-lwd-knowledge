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
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

sys.path.append("/usr/local/airflow/dags/tds/ebgi-group1/")

from config import *

VIRTUAL_CLUSTER_ID = Variable.get('VIRTUAL_CLUSTER_ID')
EXECUTION_ROLE_ARN = Variable.get('EXECUTION_ROLE_ARN')
RELEASE_LABEL = Variable.get('emr_version')
TDS_AWS_CONN_ID = Variable.get('TDS_AWS_CONN_ID')
TDS_SPARK_SUBMIT_INIT = Variable.get('TDS_SPARK_SUBMIT')
TDS_TSHIRT_SIZE = Variable.get('TDS_TSHIRT_SIZE', deserialize_json=True)
TDS_DEFAULT_TSHIRT_SIZE = Variable.get('TDS_DEFAULT_TSHIRT_SIZE', deserialize_json=True)
TDS_ADHOC_TSHIRT_SIZE = Variable.get('TDS_ADHOC_TSHIRT_SIZE', deserialize_json=True)
LOG_GROUP_NAME = "/aws/emr-eks-spark/tds"
s3_log_base_uri = Variable.get('emr_s3_log_base_uri')
s3_log_uri = f"{s3_log_base_uri}/tds/"
configuration_overrides={
                    "monitoringConfiguration": {
                        "s3MonitoringConfiguration": {
                            "logUri": s3_log_uri
                        }
                    },
                }

daily_dq_query_s3_path = Variable.get('daily_dq_query_s3_path')
rl_target_bucket_name = Variable.get("TARGET_BUCKET_NAME")

now_utc = datetime.now(timezone.utc) + timedelta(hours=8)
date=now_utc.date()
year=str(date.year)
month=str(date.month).zfill(2)
day=str(date.day).zfill(2)
run_timestamp=str(now_utc.strftime('%Y%m%d%H%M%S') + f"{now_utc.microsecond // 1000:03d}")

# Define default_args
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 3,
    "catchup": False,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
}

tshirt_size = TDS_DEFAULT_TSHIRT_SIZE

def get_tshirt_conf(size):
    if size in list(tshirt_size.keys()):
        return tshirt_size[size]
    else: 
        return tshirt_size['small']

with DAG(
    dag_id       = "datalake-tds-batch-pipeline-ebgi-group1",
    start_date   = datetime(2023, 3, 25),
    schedule = Variable.get("emr_master_dag_schedule_de"),
    default_args = default_args,
    catchup=False,
    max_active_tasks=15,
    is_paused_upon_creation=False,
    description  = 'Datalake Query Generator and Execution') as dag:

    start = DummyOperator(task_id = "start", dag=dag)
    end   = DummyOperator(task_id = "end", dag=dag)
    
    sensor_tds = ExternalTaskSensor(
        task_id='external_sensor_task_tds_ebgi_group1',
        external_dag_id='datalake_dms_master_de_ebaogi_batch',  # Specify the DAG  ID
        external_task_id='end_master',  # Specify the task in DAG  to wait for
        poke_interval=600,  # How often to check if the task is complete (in seconds)  
        timeout=60 * 60 * 8,  # Set a timeout (in seconds) for the sensor, e.g., 2 hours
        dag=dag,
    )


    for t_list in table_list:
        src_schema = t_list["src_schema"]
        src_table  = t_list["src_table"]
        tgt_schema = t_list["tgt_schema"]
        tgt_table  = t_list["tgt_table"]
        resource_size = t_list["resource_size"]
        task_group=f"tds_{tgt_table}"

        spark_conf = get_tshirt_conf(resource_size)
        spark_conf_found = False
        TDS_SPARK_SUBMIT = TDS_SPARK_SUBMIT_INIT
        
        TDS_SPARK_CONF_OPTIONS ={}
        if TDS_ADHOC_TSHIRT_SIZE:
            for ADHOC_SIZE in TDS_ADHOC_TSHIRT_SIZE:
                if ADHOC_SIZE["tgt_schema"] == tgt_schema and ADHOC_SIZE["tgt_table"] == tgt_table:
                    spark_conf = ADHOC_SIZE["adhoc_resource"]
                    TDS_SPARK_SUBMIT = ADHOC_SIZE["TDS_SPARK_SUBMIT"]
                    TDS_SPARK_CONF_OPTIONS = ADHOC_SIZE["TDS_SPARK_CONF_OPTIONS"]
                    spark_conf_found = True
                    break
        if not spark_conf_found and TDS_TSHIRT_SIZE:
            for TDS_SIZE in TDS_TSHIRT_SIZE:
                if TDS_SIZE["tgt_schema"] == tgt_schema and TDS_SIZE["tgt_table"] == tgt_table:
                    spark_conf = get_tshirt_conf(TDS_SIZE["size"])
                    break

        with TaskGroup(group_id=task_group) as task_group:

            tds_transform=EmrContainerOperator(
                task_id  = f"tds_{tgt_table}_transform",
                name = (f"tl-{tgt_table}-transformation")[:64],
                virtual_cluster_id=VIRTUAL_CLUSTER_ID,
                execution_role_arn=EXECUTION_ROLE_ARN,
                release_label=RELEASE_LABEL,        
                job_driver = {
                "sparkSubmitJobDriver": {
                "entryPoint": SCRIPT_LOCATION_TL,
                "entryPointArguments": ["--config",f"{get_table_config(src_schema, src_table, tgt_schema, tgt_table)}","--jsonfile",f"s3://{TDS_JOB_FILE_BUCKET_NAME}/tds/config/ebgi/{tgt_table}_config.json","--spark_conf_options", str(TDS_SPARK_CONF_OPTIONS)],
                "sparkSubmitParameters":f"{TDS_SPARK_SUBMIT} --conf spark.dynamicAllocation.maxExecutors={spark_conf['maxExecutors']} --conf spark.dynamicAllocation.minExecutors={spark_conf['minExecutors']} --conf spark.driver.cores={spark_conf['driver_cores']} --conf spark.driver.memory={spark_conf['driver_memory']}  --conf spark.executor.memory={spark_conf['executor_memory']} --conf spark.executor.cores={spark_conf['executor_cores']}"
                }
                },
                configuration_overrides=configuration_overrides,
                aws_conn_id=TDS_AWS_CONN_ID,        
                dag=dag
            )
            
            tds_merge=EmrContainerOperator(
                task_id  = f"tds_{tgt_table}_merge",
                name = (f"tl-{tgt_table}-merge")[:64],
                virtual_cluster_id=VIRTUAL_CLUSTER_ID,
                execution_role_arn=EXECUTION_ROLE_ARN,
                release_label=RELEASE_LABEL,        
                job_driver = {
                "sparkSubmitJobDriver": {
                "entryPoint": SCRIPT_LOCATION_FL,
                "entryPointArguments": ["--config",f"{get_table_config(src_schema, src_table, tgt_schema, tgt_table)}","--spark_conf_options", str(TDS_SPARK_CONF_OPTIONS)],
                "sparkSubmitParameters":f"{TDS_SPARK_SUBMIT} --conf spark.dynamicAllocation.maxExecutors={spark_conf['maxExecutors']} --conf spark.dynamicAllocation.minExecutors={spark_conf['minExecutors']} --conf spark.driver.cores={spark_conf['driver_cores']} --conf spark.driver.memory={spark_conf['driver_memory']}  --conf spark.executor.memory={spark_conf['executor_memory']} --conf spark.executor.cores={spark_conf['executor_cores']}"
                }
                },
                configuration_overrides=configuration_overrides,
                aws_conn_id=TDS_AWS_CONN_ID,
                dag=dag
            )

            tds_dq_compare = AthenaOperator(
                task_id  = f"tds_{tgt_table}_dq",
                query=f"""
                UNLOAD (
                SELECT  LOWER('{tgt_schema.split("_")[1]}') as app_name,
                        LOWER('{src_schema}') as src_schema,
                        LOWER('{src_table}')  as src_table,
                        LOWER('{tgt_schema}') as tgt_schema,
                        LOWER('{tgt_table}')  as tgt_table,
                        'count' as entity,
                        '*' as instance,
                        'count' as check_type,
                        cast(Records as decimal(25,2)) as records, 
                        cast(MatchedRecord as decimal(25,2))  as matched_records,
                        cast(case when MatchedRecord=Records then 0 when MatchedRecord!=Records then Records-MatchedRecord END as decimal(25,2)) as diff,
                        cast(case when MatchedRecord=Records then 0 when MatchedRecord!=Records then((Records-MatchedRecord)/Records)* 100 END as decimal(25,2)) as diff_percentage,    
                        CASE 
                            WHEN MatchedRecord=Records THEN 'Success' when abs(((Records-MatchedRecord)/Records)* 100) <= 100-99.95 THEN 'Success'
                            ELSE 'Failure'
                            END as status,
                        cast(date_add('hour', 8, current_timestamp) as timestamp) as report_date,
                        date_add('day', -1, current_date) as run_date,
                        'count check for tds' as hint
                FROM (
                        SELECT (select count(1) as count from (
                            select *, row_number() over (partition by business_key order by cdc_source_commit_date desc) rnk
                            from {src_schema}.{src_table}
                            )
                            WHERE rnk = 1
                            AND dml_ind <> 'D') as Records,
                                ( select count(1) as count from {tgt_schema}.{tgt_table} where active_record_ind='Y') as MatchedRecord)
                ) 
                TO 's3://{rl_target_bucket_name}/audit/dms_dq_report/year={year}/month={month}/day={day}/run_timestamp={run_timestamp}/'
                WITH (format = 'PARQUET')
                """,
                database='audit',
                output_location=daily_dq_query_s3_path,
                aws_conn_id=TDS_AWS_CONN_ID,
                pool="dq_pool"
            )
            
            tds_transform >> tds_merge >> tds_dq_compare

        sensor_tds >> start >> task_group >> end

if __name__ == "__main__":
    dag.cli()
