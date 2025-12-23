BEGIN;

SET TIMEZONE = 'Singapore';
	
-- Creating one temptable by union both finaltemp and final table to check whether businesskey is already present or not in final table

CREATE TABLE #tempstgdimisclaim AS SELECT source_app_code ,source_data_set, dml_ind ,  
case when latest_record_created_date is null then record_created_date else latest_record_created_date END AS record_created_date, 
record_updated_date, 
record_created_by, 
record_updated_by, 
record_eff_from_date, 
CASE WHEN dml_ind <> 'D' 
AND claim_uuid IS NULL THEN date_trunc(
  'second', 
  to_timestamp('9999-12-31', 'yyyy-MM-dd')
) WHEN dml_ind <> 'D' 
AND claim_uuid IS NOT NULL 
AND rnk = 1 THEN date_trunc(
  'second', 
  to_timestamp('9999-12-31', 'yyyy-MM-dd')
) WHEN dml_ind = 'D' then record_eff_from_date ELSE latest_record_eff_from_date END AS record_eff_to_date, 
CASE WHEN dml_ind <> 'D' 
AND claim_uuid IS NULL THEN 'Y' WHEN dml_ind <> 'D' 
AND claim_uuid IS NOT NULL 
AND rnk = 1 THEN 'Y' WHEN dml_ind = 'D' then 'N' ELSE 'N' END AS 	
			active_record_ind, 
			claim_uuid, 
			business_key, 
			claim_event_id
			,claim_id
			,claim_event_no
			,claim_no
			,claim_event_occurance_date
			,claim_event_location
			,claim_event_reporting_24hrs
			,claim_event_type_id
			,claim_type_id
			,claim_event_status_id
			,claim_status_id
			,claim_event_closed_date
			,claim_closed_date
			,country_of_accident
			,notification_date
			,claim_reported_date
			,claim_level_id
			,claim_source_id
			,claim_damage_type_id
			,claim_injury_type_id
			,towing_required
			,tp_clinic_uuid
			,tp_driver_nric
			,tp_insurer_uuid
			,tp_lawyer_uuid
			,tp_surveyor_uuid
			,tp_vehicle_no
			,tp_workshop_uuid
			,workshop_repairer
			,no_adl
			,claim_insured_liability
			,claim_insured_liability_desc
			,claim_desc
			,name_of_preferred_workshop
			,orange_force
			,our_lawyer
			,claim_submission_type
			,icm_no
			,tca
			,wbcs_flag
			,ecode
			,ecode_reason
			,od_excess
			,tp_excess
			,additional_excess
			,windscreen_excess
			,unnamed_driver_excess
			,claim_officer_staff_uuid
			,claim_creator_staff_uuid
			,wbcs_claim_no
			,claim_status_loss
			,claim_status_recovery
			,claim_status_salvage
			,cause_of_loss
			,policy_uuid
			,customer_uuid
			,product_uuid
			,sub_product_uuid
			,sales_agent_uuid
			,sales_agent_code
			,servicing_agent_uuid
			,claim_occurrance_time_hhmm
			,claimant_type_id
			,gst_registered
			,gst_verified
			,claim_event_type_code
			,claim_type_code
			,claim_event_status_code
			,claim_status_code
			,policy_id
			,policy_no
			,checksum, 
			table_type, 
			latest_record_eff_from_date, 
			rnk 
			FROM 
			(
    SELECT 
      temp.*, 
      ROW_NUMBER() OVER (
        PARTITION BY temp.claim_uuid 
        ORDER BY 
          temp.record_eff_from_date DESC
      ) AS rnk, 
      LAG(record_eff_from_date) OVER (
        PARTITION BY temp.claim_uuid 
        ORDER BY 
          temp.record_eff_from_date DESC
      ) AS latest_record_eff_from_date,  
      LEAD(record_created_date) OVER (
        PARTITION BY business_key 
        ORDER BY 
          record_eff_from_date DESC
      ) AS latest_record_created_date 
    FROM 
      (
        SELECT 
          source_app_code, 
          source_data_set, 
          dml_ind, 
          record_created_date, 
          record_updated_date, 
          record_created_by, 
          record_updated_by, 
          record_eff_from_date, 
          record_eff_to_date, 
          active_record_ind, 
          claim_uuid, 
          business_key, 
         claim_event_id
			,claim_id
			,claim_event_no
			,claim_no
			,claim_event_occurance_date
			,claim_event_location
			,claim_event_reporting_24hrs
			,claim_event_type_id
			,claim_type_id
			,claim_event_status_id
			,claim_status_id
			,claim_event_closed_date
			,claim_closed_date
			,country_of_accident
			,notification_date
			,claim_reported_date
			,claim_level_id
			,claim_source_id
			,claim_damage_type_id
			,claim_injury_type_id
			,towing_required
			,tp_clinic_uuid
			,tp_driver_nric
			,tp_insurer_uuid
			,tp_lawyer_uuid
			,tp_surveyor_uuid
			,tp_vehicle_no
			,tp_workshop_uuid
			,workshop_repairer
			,no_adl
			,claim_insured_liability
			,claim_insured_liability_desc
			,claim_desc
			,name_of_preferred_workshop
			,orange_force
			,our_lawyer
			,claim_submission_type
			,icm_no
			,tca
			,wbcs_flag
			,ecode
			,ecode_reason
			,od_excess
			,tp_excess
			,additional_excess
			,windscreen_excess
			,unnamed_driver_excess
			,claim_officer_staff_uuid
			,claim_creator_staff_uuid
			,wbcs_claim_no
			,claim_status_loss
			,claim_status_recovery
			,claim_status_salvage
			,cause_of_loss
			,policy_uuid
			,customer_uuid
			,product_uuid
			,sub_product_uuid
			,sales_agent_uuid
			,sales_agent_code
			,servicing_agent_uuid
			,claim_occurrance_time_hhmm
			,claimant_type_id
			,gst_registered
			,gst_verified
			,claim_event_type_code
			,claim_type_code
			,claim_event_status_code
			,claim_status_code
			,policy_id
			,policy_no
          ,checksum, 
          'TEMP' as table_type 
        FROM 
          el_eds_def_stg.stgdimclaim where source_app_code = 'WBCS'
        UNION 
          (
            SELECT 
              source_app_code, 
              source_data_set, 
              dml_ind, 
              record_created_date, 
              record_updated_date, 
              record_created_by, 
              record_updated_by, 
              record_eff_from_date, 
              record_eff_to_date, 
              active_record_ind, 
              claim_uuid, 
              business_key, 
              claim_event_id
			,claim_id
			,claim_event_no
			,claim_no
			,claim_event_occurance_date
			,claim_event_location
			,claim_event_reporting_24hrs
			,claim_event_type_id
			,claim_type_id
			,claim_event_status_id
			,claim_status_id
			,claim_event_closed_date
			,claim_closed_date
			,country_of_accident
			,notification_date
			,claim_reported_date
			,claim_level_id
			,claim_source_id
			,claim_damage_type_id
			,claim_injury_type_id
			,towing_required
			,tp_clinic_uuid
			,tp_driver_nric
			,tp_insurer_uuid
			,tp_lawyer_uuid
			,tp_surveyor_uuid
			,tp_vehicle_no
			,tp_workshop_uuid
			,workshop_repairer
			,no_adl
			,claim_insured_liability
			,claim_insured_liability_desc
			,claim_desc
			,name_of_preferred_workshop
			,orange_force
			,our_lawyer
			,claim_submission_type
			,icm_no
			,tca
			,wbcs_flag
			,ecode
			,ecode_reason
			,od_excess
			,tp_excess
			,additional_excess
			,windscreen_excess
			,unnamed_driver_excess
			,claim_officer_staff_uuid
			,claim_creator_staff_uuid
			,wbcs_claim_no
			,claim_status_loss
			,claim_status_recovery
			,claim_status_salvage
			,cause_of_loss
			,policy_uuid
			,customer_uuid
			,product_uuid
			,sub_product_uuid
			,sales_agent_uuid
			,sales_agent_code
			,servicing_agent_uuid
			,claim_occurrance_time_hhmm
			,claimant_type_id
			,gst_registered
			,gst_verified
			,claim_event_type_code
			,claim_type_code
			,claim_event_status_code
			,claim_status_code
			,policy_id
			,policy_no,
              checksum, 
              'HIST' as table_type 
            FROM 
              el_eds_def.dimclaim b 
            where 
              b.business_key in (
                SELECT 
                  business_key 
                FROM 
                  el_eds_def_stg.stgdimclaim a 
                where 
                  a.record_eff_from_date <> b.record_eff_from_date and a.source_app_code = 'WBCS'
              ) 
              AND b.active_record_ind = 'Y' 
              and b.source_app_code = 'WBCS'
          )
      ) temp
  ) temp;

--Merging the temp table with final table




MERGE INTO el_eds_def.dimclaim  
USING #tempstgdimisclaim temp ON el_eds_def.dimclaim.business_key = temp.business_key 
AND el_eds_def.dimclaim.record_eff_from_date= temp.record_eff_from_date and el_eds_def.dimclaim.source_app_code ='WBCS' 
WHEN MATCHED 
THEN UPDATE	
		set
		record_updated_date= temp.record_updated_date
		,record_eff_to_date = case WHEN el_eds_def.dimclaim.active_record_ind = 'Y' AND temp.rnk != 1 then temp.latest_record_eff_from_date else el_eds_def.dimclaim.record_eff_to_date END
		,active_record_ind = case WHEN el_eds_def.dimclaim.active_record_ind = 'Y' AND temp.rnk != 1 then temp.active_record_ind else el_eds_def.dimclaim.active_record_ind END
		,claim_event_id=temp.claim_event_id
		,claim_id=temp.claim_id
		,claim_event_no=temp.claim_event_no
		,claim_no=temp.claim_no
		,claim_event_occurance_date=temp.claim_event_occurance_date
		,claim_event_location=temp.claim_event_location
		,claim_event_reporting_24hrs=temp.claim_event_reporting_24hrs
		,claim_event_type_id=temp.claim_event_type_id
		,claim_type_id=temp.claim_type_id
		,claim_event_status_id=temp.claim_event_status_id
		,claim_status_id=temp.claim_status_id
		,claim_event_closed_date=temp.claim_event_closed_date
		,claim_closed_date=temp.claim_closed_date
		,country_of_accident=temp.country_of_accident
		,notification_date=temp.notification_date
		,claim_reported_date=temp.claim_reported_date
		,claim_level_id=temp.claim_level_id
		,claim_source_id=temp.claim_source_id
		,claim_damage_type_id=temp.claim_damage_type_id
		,claim_injury_type_id=temp.claim_injury_type_id
		,towing_required=temp.towing_required
		,tp_clinic_uuid=temp.tp_clinic_uuid
		,tp_driver_nric=temp.tp_driver_nric
		,tp_insurer_uuid=temp.tp_insurer_uuid
		,tp_lawyer_uuid=temp.tp_lawyer_uuid
		,tp_surveyor_uuid=temp.tp_surveyor_uuid
		,tp_vehicle_no=temp.tp_vehicle_no
		,tp_workshop_uuid=temp.tp_workshop_uuid
		,workshop_repairer=temp.workshop_repairer
		,no_adl=temp.no_adl
		,claim_insured_liability=temp.claim_insured_liability
		,claim_insured_liability_desc=temp.claim_insured_liability_desc
		,claim_desc=temp.claim_desc
		,name_of_preferred_workshop=temp.name_of_preferred_workshop
		,orange_force=temp.orange_force
		,our_lawyer=temp.our_lawyer
		,claim_submission_type=temp.claim_submission_type
		,icm_no=temp.icm_no
		,tca=temp.tca
		,wbcs_flag=temp.wbcs_flag
		,ecode=temp.ecode
		,ecode_reason=temp.ecode_reason
		,od_excess=temp.od_excess
		,tp_excess=temp.tp_excess
		,additional_excess=temp.additional_excess
		,windscreen_excess=temp.windscreen_excess
		,unnamed_driver_excess=temp.unnamed_driver_excess
		,claim_officer_staff_uuid=temp.claim_officer_staff_uuid
		,claim_creator_staff_uuid=temp.claim_creator_staff_uuid
		,wbcs_claim_no=temp.wbcs_claim_no
		,claim_status_loss=temp.claim_status_loss
		,claim_status_recovery=temp.claim_status_recovery
		,claim_status_salvage=temp.claim_status_salvage
		,cause_of_loss=temp.cause_of_loss
		,policy_uuid=temp.policy_uuid
		,customer_uuid=temp.customer_uuid
		,product_uuid=temp.product_uuid
		,sub_product_uuid=temp.sub_product_uuid
		,sales_agent_uuid=temp.sales_agent_uuid
		,sales_agent_code=temp.sales_agent_code
		,servicing_agent_uuid=temp.servicing_agent_uuid
		,claim_occurrance_time_hhmm=temp.claim_occurrance_time_hhmm
		,claimant_type_id=temp.claimant_type_id
		,gst_registered=temp.gst_registered
		,gst_verified=temp.gst_verified
		,claim_event_type_code=temp.claim_event_type_code
		,claim_type_code=temp.claim_type_code
		,claim_event_status_code=temp.claim_event_status_code
		,claim_status_code=temp.claim_status_code
		,policy_id=temp.policy_id
		,policy_no=temp.policy_no
		,checksum=temp.checksum
WHEN NOT MATCHED
THEN insert (source_app_code,source_data_set,dml_ind ,record_created_date
		 ,record_updated_date,record_created_by ,record_updated_by ,record_eff_from_date ,record_eff_to_date,active_record_ind,
		claim_uuid ,business_key 
		,claim_event_id
		,claim_id
		,claim_event_no
		,claim_no
		,claim_event_occurance_date
		,claim_event_location
		,claim_event_reporting_24hrs
		,claim_event_type_id
		,claim_type_id
		,claim_event_status_id
		,claim_status_id
		,claim_event_closed_date
		,claim_closed_date
		,country_of_accident
		,notification_date
		,claim_reported_date
		,claim_level_id
		,claim_source_id
		,claim_damage_type_id
		,claim_injury_type_id
		,towing_required
		,tp_clinic_uuid
		,tp_driver_nric
		,tp_insurer_uuid
		,tp_lawyer_uuid
		,tp_surveyor_uuid
		,tp_vehicle_no
		,tp_workshop_uuid
		,workshop_repairer
		,no_adl
		,claim_insured_liability
		,claim_insured_liability_desc
		,claim_desc
		,name_of_preferred_workshop
		,orange_force
		,our_lawyer
		,claim_submission_type
		,icm_no
		,tca
		,wbcs_flag
		,ecode
		,ecode_reason
		,od_excess
		,tp_excess
		,additional_excess
		,windscreen_excess
		,unnamed_driver_excess
		,claim_officer_staff_uuid
		,claim_creator_staff_uuid
		,wbcs_claim_no
		,claim_status_loss
		,claim_status_recovery
		,claim_status_salvage
		,cause_of_loss
		,policy_uuid
		,customer_uuid
		,product_uuid
		,sub_product_uuid
		,sales_agent_uuid
		,sales_agent_code
		,servicing_agent_uuid
		,claim_occurrance_time_hhmm
		,claimant_type_id
		,gst_registered
		,gst_verified
		,claim_event_type_code
		,claim_type_code
		,claim_event_status_code
		,claim_status_code
		,policy_id
		,policy_no
		,checksum)
		values(temp.source_app_code,temp.source_data_set,temp.dml_ind ,temp.record_created_date
		 ,temp.record_updated_date,temp.record_created_by ,temp.record_updated_by ,temp.record_eff_from_date ,temp.record_eff_to_date,temp.active_record_ind,
		temp.claim_uuid ,temp.business_key 
		,temp.claim_event_id
		,temp.claim_id
		,temp.claim_event_no
		,temp.claim_no
		,temp.claim_event_occurance_date
		,temp.claim_event_location
		,temp.claim_event_reporting_24hrs
		,temp.claim_event_type_id
		,temp.claim_type_id
		,temp.claim_event_status_id
		,temp.claim_status_id
		,temp.claim_event_closed_date
		,temp.claim_closed_date
		,temp.country_of_accident
		,temp.notification_date
		,temp.claim_reported_date
		,temp.claim_level_id
		,temp.claim_source_id
		,temp.claim_damage_type_id
		,temp.claim_injury_type_id
		,temp.towing_required
		,temp.tp_clinic_uuid
		,temp.tp_driver_nric
		,temp.tp_insurer_uuid
		,temp.tp_lawyer_uuid
		,temp.tp_surveyor_uuid
		,temp.tp_vehicle_no
		,temp.tp_workshop_uuid
		,temp.workshop_repairer
		,temp.no_adl
		,temp.claim_insured_liability
		,temp.claim_insured_liability_desc
		,temp.claim_desc
		,temp.name_of_preferred_workshop
		,temp.orange_force
		,temp.our_lawyer
		,temp.claim_submission_type
		,temp.icm_no
		,temp.tca
		,temp.wbcs_flag
		,temp.ecode
		,temp.ecode_reason
		,temp.od_excess
		,temp.tp_excess
		,temp.additional_excess
		,temp.windscreen_excess
		,temp.unnamed_driver_excess
		,temp.claim_officer_staff_uuid
		,temp.claim_creator_staff_uuid
		,temp.wbcs_claim_no
		,temp.claim_status_loss
		,temp.claim_status_recovery
		,temp.claim_status_salvage
		,temp.cause_of_loss
		,temp.policy_uuid
		,temp.customer_uuid
		,temp.product_uuid
		,temp.sub_product_uuid
		,temp.sales_agent_uuid
		,temp.sales_agent_code
		,temp.servicing_agent_uuid
		,temp.claim_occurrance_time_hhmm
		,temp.claimant_type_id
		,temp.gst_registered
		,temp.gst_verified
		,temp.claim_event_type_code
		,temp.claim_type_code
		,temp.claim_event_status_code
		,temp.claim_status_code
		,temp.policy_id
		,temp.policy_no
		,temp.checksum);

DROP TABLE IF EXISTS #tempstgdimisclaim;

END;