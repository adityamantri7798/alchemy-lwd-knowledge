BEGIN;
SET TIMEZONE = 'Singapore';		
-- Creating one temptable by union both finaltemp and final table to check whether businesskey is already present or not in final table

CREATE TABLE #tempstgfactpolicybonusdistribution
 AS 
 SELECT 
	source_app_code 
	,source_data_set 
	,dml_ind 
	, case when latest_record_created_date is null then record_created_date else latest_record_created_date END AS record_created_date
	,record_updated_date 
	,record_created_by 
	,record_updated_by 
	,record_eff_from_date 
	,CASE 
          WHEN dml_ind <>'D' AND policy_bonus_uuid IS NULL 
          THEN date_trunc('second', to_timestamp('9999-12-31', 'yyyy-MM-dd'))
          WHEN dml_ind <>'D' AND policy_bonus_uuid IS NOT NULL AND rnk=1
          THEN date_trunc('second', to_timestamp('9999-12-31', 'yyyy-MM-dd'))
          WHEN dml_ind='D' then record_eff_from_date
          ELSE latest_record_eff_from_date END  AS record_eff_to_date , 
	CASE 
          WHEN dml_ind <> 'D' AND policy_bonus_uuid IS NULL
          THEN 'Y'
          WHEN dml_ind <> 'D' AND policy_bonus_uuid IS NOT NULL AND rnk=1  
          THEN 'Y'
          WHEN dml_ind  = 'D' then 'N'
          ELSE 'N'
	END AS active_record_ind 
	,checksum
	,policy_bonus_uuid
	,business_key
	,policy_no
	,policy_id
	,policy_plan_id
	,bonus_allocation_id
	,bonus_allocation_date
	,distribution_type
	,bonus_allocation_method
	,bonus_amount
	,endt_id
	,policy_uuid
	,policy_covered_item_uuid
	,business_data_set
	,table_type
	,latest_record_eff_from_date
	,rnk
         FROM(SELECT temp.*,
                 ROW_NUMBER() OVER (PARTITION BY temp.policy_bonus_uuid ORDER BY temp.record_eff_from_date DESC) AS rnk,
                 LAG(record_eff_from_date) OVER (PARTITION BY temp.policy_bonus_uuid ORDER BY temp.record_eff_from_date DESC) AS latest_record_eff_from_date,
		LEAD(record_created_date) OVER ( PARTITION BY business_key ORDER BY record_eff_from_date DESC) AS latest_record_created_date FROM
                 (
                 SELECT source_app_code
			,source_data_set
			,dml_ind
			,record_created_date
			,record_updated_date
			,record_created_by
			,record_updated_by
			,record_eff_from_date
			,record_eff_to_date
			,active_record_ind
			,checksum
			,policy_bonus_uuid
			,business_key
			,policy_no
			,policy_id
			,policy_plan_id
			,bonus_allocation_id
			,bonus_allocation_date
			,distribution_type
			,bonus_allocation_method
			,bonus_amount
			,endt_id
			,policy_uuid
			,policy_covered_item_uuid
			,business_data_set	
			,'TEMP' as table_type FROM el_eds_def_stg.stgfactpolicybonusdistribution where source_app_code='EBLI'
                 UNION 
                 (SELECT source_app_code
			,source_data_set
			,dml_ind
			,record_created_date
			,record_updated_date
			,record_created_by
			,record_updated_by
			,record_eff_from_date
			,record_eff_to_date
			,active_record_ind
			,checksum
			,policy_bonus_uuid
			,business_key
			,policy_no
			,policy_id
			,policy_plan_id
			,bonus_allocation_id
			,bonus_allocation_date
			,distribution_type
			,bonus_allocation_method
			,bonus_amount
			,endt_id
			,policy_uuid
			,policy_covered_item_uuid
			,business_data_set	
			,'HIST' as table_type FROM  el_eds_def.factpolicybonusdistribution  b where b.business_key in (SELECT business_key 
                                              FROM el_eds_def_stg.stgfactpolicybonusdistribution  a where 
                  a.record_eff_from_date <> b.record_eff_from_date and a.source_app_code = 'EBLI')
                   AND b.active_record_ind = 'Y' and b.source_app_code = 'EBLI')
                    )temp
					 ) temp;


--Merging the temp table with final table

MERGE INTO el_eds_def.factpolicybonusdistribution  
USING #tempstgfactpolicybonusdistribution temp ON el_eds_def.factpolicybonusdistribution.business_key = temp.business_key 
AND el_eds_def.factpolicybonusdistribution.record_eff_from_date = temp.record_eff_from_date and temp.source_app_code='EBLI'
WHEN MATCHED 
THEN UPDATE	
		set
		record_updated_date= temp.record_updated_date,
		record_eff_to_date = case WHEN el_eds_def.factpolicybonusdistribution.active_record_ind = 'Y' AND temp.rnk != 1 then temp.latest_record_eff_from_date else el_eds_def.factpolicybonusdistribution.record_eff_to_date END,
		active_record_ind = case WHEN el_eds_def.factpolicybonusdistribution.active_record_ind = 'Y' AND temp.rnk != 1 then temp.active_record_ind else el_eds_def.factpolicybonusdistribution.active_record_ind END,
		checksum =temp.checksum,
		policy_bonus_uuid = temp.policy_bonus_uuid,
		business_key= temp.business_key,
		policy_no  = temp.policy_no,
		policy_id  = temp.policy_id,
		policy_plan_id= temp.policy_plan_id,
		bonus_allocation_id= temp.bonus_allocation_id,
		bonus_allocation_date = temp.bonus_allocation_date,
		distribution_type  = temp.distribution_type,
		bonus_allocation_method  = temp.bonus_allocation_method,
		bonus_amount  = temp.bonus_amount,
		endt_id = temp.endt_id,
		policy_uuid= temp.policy_uuid,
		policy_covered_item_uuid = temp.policy_covered_item_uuid,
		business_data_set= temp.business_data_set
WHEN NOT MATCHED
THEN insert (source_app_code
			,source_data_set
			,dml_ind
			,record_created_date
			,record_updated_date
			,record_created_by
			,record_updated_by
			,record_eff_from_date
			,record_eff_to_date
			,active_record_ind
			,checksum
			,policy_bonus_uuid
			,business_key
			,policy_no
			,policy_id
			,policy_plan_id
			,bonus_allocation_id
			,bonus_allocation_date
			,distribution_type
			,bonus_allocation_method
			,bonus_amount
			,endt_id
			,policy_uuid
			,policy_covered_item_uuid
			,business_data_set	
			)
		values(temp.source_app_code
				,temp.source_data_set
				,temp.dml_ind
				,temp.record_created_date
				,temp.record_updated_date
				,temp.record_created_by
				,temp.record_updated_by
				,temp.record_eff_from_date
				,temp.record_eff_to_date
				,temp.active_record_ind
				,temp.checksum
				,temp.policy_bonus_uuid
				,temp.business_key
				,temp.policy_no
				,temp.policy_id
				,temp.policy_plan_id
				,temp.bonus_allocation_id
				,temp.bonus_allocation_date
				,temp.distribution_type
				,temp.bonus_allocation_method
				,temp.bonus_amount
				,temp.endt_id
				,temp.policy_uuid
				,temp.policy_covered_item_uuid
				,temp.business_data_set	
				);

DROP TABLE IF EXISTS #tempstgfactpolicybonusdistribution ; 
		
		
END;
