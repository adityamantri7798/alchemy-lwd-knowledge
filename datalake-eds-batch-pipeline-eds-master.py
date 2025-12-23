import os
import sys
from datetime import date, timedelta, datetime, timezone
from airflow import DAG
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.operators.dummy import DummyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

REDSHIFT_CONNECTION_ID = Variable.get("REDSHIFT_CONNECTION_ID")

DEFAULT_ARGS = {
    "owner": "datalake,eds",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catch_up": False,
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "conn_id": REDSHIFT_CONNECTION_ID, 
}

parallel_insert_lists = [
    '/usr/local/airflow/dags/eds/scripts/eds-master/dimproduct/datalake-eds-batch-pipeline-eds-master-dimproduct.sql',
    '/usr/local/airflow/dags/eds/scripts/eds-master/policy-ebgi/datalake-eds-batch-pipeline-eds-master-dimpolicy-ebgi.sql',
    '/usr/local/airflow/dags/eds/scripts/eds-master/policy-ebli/datalake-eds-batch-pipeline-eds-master-dimpolicy-ebli.sql',
    '/usr/local/airflow/dags/eds/scripts/eds-master/policycovereditem-ebgi/datalake-eds-batch-pipeline-eds-master-dimpolicycovereditem-ebgi.sql',
    '/usr/local/airflow/dags/eds/scripts/eds-master/policycovereditem-ebli/datalake-eds-batch-pipeline-eds-master-dimpolicycovereditem-ebli.sql',
    '/usr/local/airflow/dags/eds/scripts/eds-master/policy-ebgi/datalake-eds-batch-pipeline-eds-master-factpolicy-ebgi.sql',
    '/usr/local/airflow/dags/eds/scripts/eds-master/policy-ebli/datalake-eds-batch-pipeline-eds-master-factpolicy-ebli.sql',
    '/usr/local/airflow/dags/eds/scripts/eds-master/policycovereditem-ebgi/datalake-eds-batch-pipeline-eds-master-factpolicycovereditem-ebgi.sql',
    '/usr/local/airflow/dags/eds/scripts/eds-master/policycovereditem-ebli/datalake-eds-batch-pipeline-eds-master-factpolicycovereditem-ebli.sql',
    '/usr/local/airflow/dags/eds/scripts/eds-master/dimpolicycertificate/datalake-eds-batch-pipeline-eds-master-dimpolicycertificate-ebgi.sql',
    '/usr/local/airflow/dags/eds/scripts/eds-master/dimcustomer/datalake-eds-batch-pipeline-eds-master-dimcustomer.sql',
    '/usr/local/airflow/dags/eds/scripts/eds-master/dimcustomeraddress/datalake-eds-batch-pipeline-eds-master-dimcustomeraddress.sql',
    '/usr/local/airflow/dags/eds/scripts/eds-master/diminsuredentity/datalake-eds-batch-pipeline-eds-master-diminsuredentity.sql',
    '/usr/local/airflow/dags/eds/scripts/eds-master/proposal-ebli/datalake-eds-batch-pipeline-eds-master-dimproposal-ebli.sql',
    '/usr/local/airflow/dags/eds/scripts/eds-master/proposal-ebli/datalake-eds-batch-pipeline-eds-master-factproposal-ebli.sql',
    '/usr/local/airflow/dags/eds/scripts/eds-master/proposalcovereditem-ebli/datalake-eds-batch-pipeline-eds-master-dimproposalcovereditem-ebli.sql',
    '/usr/local/airflow/dags/eds/scripts/eds-master/proposalcovereditem-ebli/datalake-eds-batch-pipeline-eds-master-factproposalcovereditem-ebli.sql'
]


parallel_audit_lists = [
    '/usr/local/airflow/dags/eds/scripts/eds-master/dimproduct/datalake-eds-batch-pipeline-eds-master-dimproduct-audit.sql',
    '/usr/local/airflow/dags/eds/scripts/eds-master/policy-ebgi/datalake-eds-batch-pipeline-eds-master-dimpolicy-ebgi-audit.sql',
    '/usr/local/airflow/dags/eds/scripts/eds-master/policy-ebli/datalake-eds-batch-pipeline-eds-master-dimpolicy-ebli-audit.sql',
    '/usr/local/airflow/dags/eds/scripts/eds-master/policycovereditem-ebgi/datalake-eds-batch-pipeline-eds-master-dimpolicycovereditem-ebgi-audit.sql',
    '/usr/local/airflow/dags/eds/scripts/eds-master/policycovereditem-ebli/datalake-eds-batch-pipeline-eds-master-dimpolicycovereditem-ebli-audit.sql',
    '/usr/local/airflow/dags/eds/scripts/eds-master/policy-ebgi/datalake-eds-batch-pipeline-eds-master-factpolicy-ebgi-audit.sql',
    '/usr/local/airflow/dags/eds/scripts/eds-master/policy-ebli/datalake-eds-batch-pipeline-eds-master-factpolicy-ebli-audit.sql',
    '/usr/local/airflow/dags/eds/scripts/eds-master/policycovereditem-ebgi/datalake-eds-batch-pipeline-eds-master-factpolicycovereditem-ebgi-audit.sql',
    '/usr/local/airflow/dags/eds/scripts/eds-master/policycovereditem-ebli/datalake-eds-batch-pipeline-eds-master-factpolicycovereditem-ebli-audit.sql',    
    '/usr/local/airflow/dags/eds/scripts/eds-master/dimpolicycertificate/datalake-eds-batch-pipeline-eds-master-dimpolicycertificate-ebgi-audit.sql',
    '/usr/local/airflow/dags/eds/scripts/eds-master/dimcustomer/datalake-eds-batch-pipeline-eds-master-dimcustomer-audit.sql',
    '/usr/local/airflow/dags/eds/scripts/eds-master/dimcustomeraddress/datalake-eds-batch-pipeline-eds-master-dimcustomeraddress-audit.sql',
    '/usr/local/airflow/dags/eds/scripts/eds-master/diminsuredentity/datalake-eds-batch-pipeline-eds-master-diminsuredentity-audit.sql',
    '/usr/local/airflow/dags/eds/scripts/eds-master/proposal-ebli/datalake-eds-batch-pipeline-eds-master-dimproposal-ebli-audit.sql',
    '/usr/local/airflow/dags/eds/scripts/eds-master/proposal-ebli/datalake-eds-batch-pipeline-eds-master-factproposal-ebli-audit.sql',
    '/usr/local/airflow/dags/eds/scripts/eds-master/proposalcovereditem-ebli/datalake-eds-batch-pipeline-eds-master-dimproposalcovereditem-ebli-audit.sql',
    '/usr/local/airflow/dags/eds/scripts/eds-master/proposalcovereditem-ebli/datalake-eds-batch-pipeline-eds-master-factproposalcovereditem-ebli-audit.sql'
]


with DAG(
    dag_id="datalake-eds-batch-pipeline-eds-master",
    description="",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 8, 22),
    schedule_interval=Variable.get("emr_master_dag_schedule_de"),
    tags=[""],
    catchup=False,
    max_active_tasks=20,
    is_paused_upon_creation=False,
) as dag:
    
    start_task = DummyOperator(task_id='start', dag=dag)
    end_task = DummyOperator(task_id='end', dag=dag)
    
    sensor_eds_master1 = ExternalTaskSensor(
        task_id='external_sensor_task_eds_master1',
        external_dag_id='datalake-eds-batch-pipeline-master1',  # Specify the DAG ID
        external_task_id='end',  # Specify the task in DAG to wait for    
        poke_interval=300,  # How often to check if the task is complete (in seconds)  
        timeout=60 * 60 * 8,  # Set a timeout (in seconds) for the sensor
        dag=dag,
    )


    with TaskGroup(group_id='eds_master_insert_group') as eds_master_insert_group:
        insert_tasks = []
        for sql_list in parallel_insert_lists:
            with open(sql_list, 'r') as sql_file:
                sql = sql_file.read()
            task_id = f'{sql_list.split("/")[-1].replace(".sql","")}'
            task = SQLExecuteQueryOperator(
                task_id=task_id,
                sql=sql,
                split_statements=True
            )
            insert_tasks.append(task)
          
         
    with TaskGroup(group_id='eds_master_audit_group') as eds_master_audit_group:
        audit_tasks = []
        for sql_list in parallel_audit_lists:
            with open(sql_list, 'r') as sql_file:
                sql = sql_file.read()
            task_id = f'{sql_list.split("/")[-1].replace(".sql","")}'
            task = SQLExecuteQueryOperator(
                task_id=task_id,
                sql=sql,
                split_statements=True
            )
            audit_tasks.append(task)
           
    sensor_eds_master1 >> start_task >> eds_master_insert_group >> eds_master_audit_group >> end_task