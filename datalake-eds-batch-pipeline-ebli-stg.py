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
config = Variable.get('eds_pipeline_ebli_stg_config', deserialize_json=True)
SENSOR_CONFIG = config["sensors"]

def sensor_failure_callback(context):
    print(f"Sensor {context['task_instance'].task_id} failed")

with DAG(
   dag_id="datalake-eds-batch-pipeline-ebli-stg",
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
    ebli_group1_completed = DummyOperator(task_id='ebli_group1_completed')
    end = DummyOperator(task_id='end')

    def create_sequential_tasks(task_group, sql_files, domain_name, table):
        previous_task = None
        for sql_file in sql_files:
            with open(sql_file, 'r') as sql_file_handle:
                sql = sql_file_handle.read()
                task_name = f'{sql_file.split("/")[-1].replace(".sql","")}'
            
            task = SQLExecuteQueryOperator(
                task_id=task_name,
                sql=sql,
                split_statements=True,
                task_group=task_group,
                params={"table_schema": "el_eds_def", "table_name": table}
            )
            
            if previous_task:
                previous_task >> task
            previous_task = task
        return previous_task

    def create_domain_group(domain_name):
        with TaskGroup(group_id=domain_name) as domain_group:
            group1_tasks = []
            for table in config["task_dependencies"][domain_name]["group1"]:
                with TaskGroup(group_id=f"{table}") as table_group:
                    create_sequential_tasks(table_group, config["sql_files"][domain_name][table], domain_name, table)
                    group1_tasks.append(table_group)

            middle_groups = []
            for table in config["task_dependencies"][domain_name]["middle"]:
                with TaskGroup(group_id=f"{table}") as table_group:
                    create_sequential_tasks(table_group, config["sql_files"][domain_name][table], domain_name, table)
                    middle_groups.append(table_group)

            final_groups = []
            for table in config["task_dependencies"][domain_name]["final"]:
                with TaskGroup(group_id=f"{table}") as table_group:
                    create_sequential_tasks(table_group, config["sql_files"][domain_name][table], domain_name, table)
                    final_groups.append(table_group)

            for group1 in group1_tasks:
                for middle_group in middle_groups:
                    group1 >> middle_group
                    
            for middle_group in middle_groups:
                for final_group in final_groups:
                    middle_group >> final_group
        return domain_group

    ebli_group1 = create_domain_group("ebli_group1")
    ebli_group2 = create_domain_group("ebli_group2")
        
    sensors >> start >> ebli_group1 >> ebli_group1_completed >> ebli_group2 >> end