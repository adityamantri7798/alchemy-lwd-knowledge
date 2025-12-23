import os
from datetime import timedelta, datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup
from airflow.sensors.external_task_sensor import ExternalTaskSensor
import json

REDSHIFT_CONNECTION_ID = Variable.get("REDSHIFT_CONNECTION_ID")
 
DEFAULT_ARGS = {
   "owner": "datalake,eds",
   "depends_on_past": False,
   "retries": 5,
   "retry_delay": timedelta(minutes=2),
   "catch_up": False,
   "retry_exponential_backoff": True,
   "max_retry_delay": timedelta(minutes=10),
   "conn_id": REDSHIFT_CONNECTION_ID, 
}

metadata=Variable.get('eds_master_get_metadata_config',default_var={}, deserialize_json=True)

config = Variable.get('eds_master_pass_to_metadata_dags_config',default_var={}, deserialize_json=True)

script=config["sql_script_location"]
SENSOR_CONFIG = config["ebli"]["sensors"]

def sensor_failure_callback(context):
    print(f"Sensor {context['task_instance'].task_id} failed")

with DAG(
   dag_id="datalake-eds-batch-pipeline-master-ebli_test",
   description="",
   default_args=DEFAULT_ARGS,
   start_date=datetime(2024, 8, 22),
   schedule_interval=Variable.get("emr_master_dag_schedule_de"),
   tags=[""],
   catchup=False,
   max_active_tasks=20,
   is_paused_upon_creation=False,
) as dag:

    # Create sensors using configuration
    sensors = []
    for sensor_name, sensor_config in SENSOR_CONFIG.items():
        # Convert execution_delta dict to timedelta if it exists
        execution_delta = None
        if sensor_config['execution_delta']:
            execution_delta = timedelta(**sensor_config['execution_delta'])
        
        sensor = ExternalTaskSensor(
            task_id=f'external_sensor_task_{sensor_name}',
            external_dag_id=sensor_config['external_dag_id'],
            external_task_id=sensor_config['external_task_id'],
            poke_interval=300,
            timeout=60 * 60 * 8,
            execution_delta=execution_delta,
            on_failure_callback=sensor_failure_callback,
            dag=dag,
        )
        sensors.append(sensor)
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')


    with TaskGroup(group_id="ebli") as ebli_group:
        stage_file=config["sql_script_location"]["stage"]
        audit_file=config["sql_script_location"]["audit"]
        with open(stage_file, 'r') as sql_file_stage:
            stage_query=sql_file_stage.read()
            
        with open(audit_file, 'r') as sql_file_audit:
            audit_query=sql_file_audit.read()
            
        independent_tasks = []
        for table_name in config["ebli"]["table_list"]:
            with TaskGroup(group_id=f"{table_name}") as table_group:
                tgt_col=metadata[table_name]["tgt_col"]
                src_col=metadata[table_name]["src_col"]
                checksum=metadata[table_name]["checksum"]
                if config["ebli"]["table_list"][table_name]:
                    condition=config["ebli"]["table_list"][table_name]["condition"]
                else:
                    condition=""
                source_app_code='EBLI'
                sql_query_stage=stage_query
                sql_query_audit=audit_query
                
                task_stage = SQLExecuteQueryOperator(
                task_id=f"datalake-eds-batch-pipeline-eds-master-{table_name}",
                sql=sql_query_stage,
                split_statements=True,
                task_group=table_group,
                params={
                            "table_name": table_name,
                            "tgt_col": tgt_col,
                            "src_col": src_col,
                            "checksum": checksum,
                            "condition": condition,
                            "source_app_code": source_app_code,
                        },

                )
                
                task_audit = SQLExecuteQueryOperator(
                task_id=f"datalake-eds-batch-pipeline-eds-master-{table_name}-audit",
                sql=sql_query_audit,
                split_statements=True,
                task_group=table_group,
                params={
                            "table_name": table_name,
                            "source_app_code": source_app_code,
                            "condition": condition,
                        },
                )
                
                task_stage>>task_audit
                
                
                independent_tasks.append(table_group)
                
    sensors >> start >> ebli_group >> end
