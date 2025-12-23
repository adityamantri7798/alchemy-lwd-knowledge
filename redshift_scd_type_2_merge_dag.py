from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.exceptions import AirflowNotFoundException
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json 

logger = LoggingMixin().log

# Get connection ID from Variables with a default value
REDSHIFT_CONN_ID = Variable.get("REDSHIFT_CONNECTION_ID", default_var='redshift_default')
        
def get_column_mappings():
    """
    Get column mappings from Airflow Variable
    Returns dictionary of column mappings
    """
    try:
        # Get the mapping directly with deserialize_json=True
        column_mappings = Variable.get("EDS_MASTER_COLUMN_MAPPINGS", deserialize_json=True)
        logger.info("Successfully loaded column mappings from Airflow Variable")
        return column_mappings
    except json.JSONDecodeError as je:
        logger.error(f"JSON Decode Error in column mappings: {str(je)}")
        raise
    except Exception as e:
        logger.error(f"Error loading column mappings: {str(e)}")
        raise

def map_source_to_target_columns(table_config):
    """
    Maps source columns to target columns using the column mappings dictionary
    Returns ordered lists of source and target columns
    """
    try:
        column_mapping = table_config['columns']
        # Convert to list of tuples to maintain order
        ordered_mappings = list(column_mapping.items())
        
        # Separate source and target columns while maintaining order
        source_cols = [source_col.lower() for source_col, _ in ordered_mappings]
        target_cols = [target_col for _, target_col in ordered_mappings]
        
        return source_cols, target_cols
    except KeyError as ke:
        logger.error(f"Missing key in table configuration: {str(ke)}")
        raise
    except Exception as e:
        logger.error(f"Error in mapping columns: {str(e)}")
        raise

def scd_type_2_merge(**context):
    conn = None
    cursor = None
    
    try:
        # Get column mappings from Airflow Variable
        column_mappings = get_column_mappings()
        conn_id = REDSHIFT_CONN_ID
        if not conn_id:
            raise AirflowNotFoundException("No connection ID provided")
        
        logger.info(f"Using Redshift connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        logger.info("Successfully connected to Redshift")
        
        timestamp_query = "SELECT GETDATE() as current_timestamp"
        cursor.execute(timestamp_query)
        current_timestamp = cursor.fetchone()[0]
        
        # Calculate the technical effective dates
        technical_record_end_date = current_timestamp - timedelta(seconds=1)
        
        technical_record_start_date = current_timestamp

        # Iterate through each table in column_mappings
        for table_name, table_config in column_mappings.items():
            try:
                logger.info(f"Processing table: {table_name}")
                
                # Extract table configuration
                source_schema = table_config['source_schema']
                source_table = table_config['source_table']
                target_schema = table_config['target_schema']
                target_table = table_config['target_table']
                source_filter_column = table_config['source_filter_column']
                audit_columns = table_config['audit_columns']

                src_table = f"{source_schema}.{source_table}"
                tgt_table = f"{target_schema}.{target_table}"

                # Get source_cols,target_cols from table configuration
                source_cols, target_cols = map_source_to_target_columns(table_config)
                
                # Log column information
                logger.info(f"Source columns in sequence: {source_cols}")
                logger.info(f"Target columns in sequence: {target_cols}")
                  
                # Get operation columns(source table columns) - use in insert query to select the columns from source
                operation_columns = [col for col in source_cols if col not in ['checksum']]
                
                # Get business_columns(target table columns) - use in insert query to insert the columns in target
                business_columns = [col for col in target_cols if col not in ['checksum']]       
                business_columns_str = ', '.join(business_columns)
                
                # Get all the source columns and remove audit columns for checksum calculation
                checksum_columns = [col for col in source_cols if col not in audit_columns]
                logger.info(f"Checksum columns in sequence: {checksum_columns}")
                checksum_str = " || ".join([f"COALESCE(CAST({col} AS VARCHAR), 'null')" for col in checksum_columns])
                
                source_filter_values = "', '".join(source_filter_column)
                source_table_query = f"""
                SELECT *,SHA2({checksum_str}, 256) AS source_checksum
                FROM {source_schema}.{source_table} 
                WHERE source_app_code IN ('{source_filter_values}')
                """

                # Update query
                update_query = f"""
                UPDATE {tgt_table}
                SET technical_record_eff_to_date = '{technical_record_end_date}'::timestamp,
                    active_record_ind = 'N',
                    record_eff_to_date = staging.record_eff_to_date
                FROM ({source_table_query}) as staging
                    INNER JOIN {tgt_table} AS target ON target.business_key = staging.business_key
                WHERE target.record_eff_from_date = staging.record_eff_from_date
                    AND target.technical_record_eff_to_date = TIMESTAMP '9999-12-31'
                    AND (
                        (staging.source_checksum <> target.checksum AND target.active_record_ind = 'Y')
                        OR (staging.source_checksum = target.checksum AND staging.active_record_ind = 'N' AND staging.dml_ind='D')
                    );
                """

                # Insert query
                insert_query = f"""
                INSERT INTO {tgt_table} (
                    {business_columns_str},
                    technical_record_eff_from_date,
                    technical_record_eff_to_date,
                    checksum
                )
                WITH ranked_target AS (
                SELECT 
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY business_key 
                        ORDER BY technical_record_eff_from_date DESC
                    ) as rn
                FROM {tgt_table}
                ),
                new_records AS (
                    SELECT 
                        {', '.join([f'staging.{col}' for col in operation_columns])},
                CASE 
                    WHEN NOT EXISTS (
                        SELECT 1 FROM ranked_target WHERE rt.business_key = staging.business_key
                    ) THEN Timestamp'1900-01-01'  -- New record (both first load and incremental)
                    ELSE '{technical_record_start_date}'::timestamp  -- Update to existing record
                END as technical_record_eff_from_date,
                        case when staging.dml_ind='D' then staging.record_eff_to_date else
                        Timestamp'9999-12-31' end as technical_record_eff_to_date,
                        staging.source_checksum as checksum
                    FROM ({source_table_query}) as staging
                    LEFT JOIN ranked_target rt
                    ON staging.business_key = rt.business_key
                    AND staging.record_eff_from_date = rt.record_eff_from_date
                    WHERE rt.business_key IS NULL 
                    OR (
                        rt.rn = 1 
                        AND (
                            (staging.source_checksum <> rt.checksum AND staging.dml_ind <> 'D')
                            OR (staging.dml_ind = 'D' AND rt.active_record_ind = 'Y')
                        )
                    )
                )
                SELECT * FROM new_records;
                """

                # Execute queries
                cursor.execute(update_query)
                logger.info(f"Successfully executed update query for {table_name} with end timestamp: {technical_record_end_date}")
                
                cursor.execute(insert_query)
                logger.info(f"Successfully executed insert query for {table_name} with start timestamp: {technical_record_start_date}")
                
                conn.commit()
                logger.info(f"Successfully committed transaction for {table_name}")

            except Exception as table_error:
                logger.error(f"Error processing table {table_name}: {str(table_error)}")
                conn.rollback()
                raise

    except Exception as e:
        logger.error(f"Error in SCD Type 2 merge: {str(e)}")
        if conn:
            try:
                conn.rollback()
                logger.info("Successfully rolled back transaction")
            except Exception as rollback_error:
                logger.error(f"Error rolling back transaction: {str(rollback_error)}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'redshift_scd_type_2_merge_dag',
    default_args=default_args,
    description='A DAG to perform SCD Type 2 merge in Redshift',
    schedule_interval=None,
)

run_scd_merge = PythonOperator(
    task_id='run_scd_merge',
    python_callable=scd_type_2_merge,
    provide_context=True,
    dag=dag,
)

run_scd_merge
