BEGIN;
SET TIMEZONE = 'Singapore';
 



----- DQ---
  
create table #tgt_count as
select
  count(*) as target_count,
  source_data_set, 
  'factpolicycovereditem' as tgt_table,
  max(record_eff_from_date) as max_record_eff_from_date
from
  el_eds_def.factpolicycovereditem
where
  source_app_code = 'ISIS'
  and active_record_ind = 'Y'
group by
  source_data_set;

  
UPDATE el_eds_def_stg.dq_audit_stg
SET matched_records = b.target_count
FROM el_eds_def_stg.dq_audit_stg a 
  INNER JOIN #tgt_count b ON a.tgt_table = b.tgt_table AND a.entity = b.source_data_set
WHERE a.diff IS NULL AND a.matched_records IS NULL;


-- Insert final audit records from staging
INSERT INTO el_eds_def_stg.ctrl_audit (
  tgt_source_data_set, tgt_source_app_code, tgt_schema, tgt_table_name,
  src_record_eff_from_date, tgt_record_eff_from_date, data_pipeline_run_date, 
  record_created_date, record_updated_date
)
SELECT 
  s.tgt_source_data_set, 
  s.tgt_source_app_code, 
  s.tgt_schema, 
  s.tgt_table_name,
  s.src_record_eff_from_date, 
  t.max_record_eff_from_date as tgt_record_eff_from_date,
  GETDATE() as data_pipeline_run_date, 
  GETDATE() as record_created_date, 
  GETDATE() as record_updated_date
FROM el_eds_def_stg.ctrl_audit_stg s
JOIN #tgt_count t ON s.tgt_source_data_set = t.source_data_set
WHERE s.tgt_table_name = 'factpolicycovereditem' AND s.tgt_source_app_code = 'ISIS';


INSERT INTO el_eds_def_stg.dq_audit (
  app_name, src_schema, src_table, tgt_schema, tgt_table, entity, instance, check_type,
  records, matched_records, diff, diff_percentage, status, report_date, run_date, hint
)
SELECT app_name, src_schema, src_table, tgt_schema, tgt_table, entity, instance, check_type,
       records, matched_records,
       CASE WHEN matched_records = records THEN 0 ELSE ABS(records - matched_records) END as diff,
       CASE WHEN matched_records = records THEN 0 
            ELSE ABS((records - matched_records) * 100 / CASE WHEN COALESCE(records, 0) < 1 THEN 1 ELSE records END) END as diff_percentage,
       CASE WHEN matched_records = records THEN 'Success' 
            WHEN ABS((records - matched_records) * 100 / CASE WHEN COALESCE(records, 0) < 1 THEN 1 ELSE records END) <= 1 THEN 'Success' 
            ELSE 'Failure' END as status,
       GETDATE() as report_date, CAST(GETDATE() AS DATE) as run_date, 'select count(*) from table' as hint
FROM el_eds_def_stg.dq_audit_stg 
WHERE tgt_table = 'factpolicycovereditem' and entity in (select source_data_set from #tgt_count);

-- Cleanup staging tables
DROP TABLE IF EXISTS #tgt_count;

END;
