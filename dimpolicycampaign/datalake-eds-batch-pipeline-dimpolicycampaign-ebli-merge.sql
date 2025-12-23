BEGIN;
SET TIMEZONE = 'Singapore';

-- Creating one temptable by union both finaltemp and final table to check whether businesskey is already present or not in final table

CREATE TABLE #tempstgdimpolicycampaign AS 
SELECT 
	source_app_code,
	source_data_set,
	dml_ind, 
	CASE WHEN latest_record_created_date IS NULL 
		THEN record_created_date 
		ELSE latest_record_created_date 
	END AS record_created_date,
	record_updated_date,
	record_created_by,
	record_updated_by,
	record_eff_from_date,
	CASE 
		WHEN dml_ind <> 'D' AND policy_campaign_uuid IS NULL 
		THEN date_trunc('second', to_timestamp('9999-12-31', 'yyyy-MM-dd'))
		WHEN dml_ind <> 'D' AND policy_campaign_uuid IS NOT NULL AND rnk = 1
		THEN date_trunc('second', to_timestamp('9999-12-31', 'yyyy-MM-dd'))
		WHEN dml_ind = 'D' THEN record_eff_from_date
		ELSE latest_record_eff_from_date 
	END AS record_eff_to_date, 
	CASE 
		WHEN dml_ind <> 'D' AND policy_campaign_uuid IS NULL
		THEN 'Y'
		WHEN dml_ind <> 'D' AND policy_campaign_uuid IS NOT NULL AND rnk = 1  
		THEN 'Y'
		WHEN dml_ind = 'D' THEN 'N'
		ELSE 'N'
	END AS active_record_ind,
	policy_campaign_uuid,
    business_key,
    campaign_id,
    campaign_code,
    campaign_name,
    premium_discount_percentage,
    premium_discount_value,
    discount_type,
    policy_no,
    policy_id,
    policy_plan_id,
    policy_uuid,
    policy_covered_item_uuid,
    campaign_uuid,
    business_data_set,
	checksum,
	table_type,
	latest_record_eff_from_date,
	rnk
FROM (
	SELECT
		temp.*,
		ROW_NUMBER() OVER (
			PARTITION BY temp.policy_campaign_uuid 
			ORDER BY temp.record_eff_from_date DESC
			) AS rnk,
		LAG(record_eff_from_date) OVER (
			PARTITION BY temp.policy_campaign_uuid 
			ORDER BY temp.record_eff_from_date DESC
			) AS latest_record_eff_from_date,
		LEAD(record_created_date) OVER (
			PARTITION BY business_key 
			ORDER BY record_eff_from_date DESC
			) AS latest_record_created_date
	FROM (
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
			policy_campaign_uuid,
            business_key,
            campaign_id,
            campaign_code,
            campaign_name,
            premium_discount_percentage,
            premium_discount_value,
            discount_type,
            policy_no,
            policy_id,
            policy_plan_id,
            policy_uuid,
            policy_covered_item_uuid,
            campaign_uuid,
            business_data_set,
			checksum,
			'TEMP' AS table_type
		FROM
			el_eds_def_stg.stgdimpolicycampaign WHERE source_app_code = 'EBLI'
		UNION
		(SELECT
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
			policy_campaign_uuid,
            business_key,
            campaign_id,
            campaign_code,
            campaign_name,
            premium_discount_percentage,
            premium_discount_value,
            discount_type,
            policy_no,
            policy_id,
            policy_plan_id,
            policy_uuid,
            policy_covered_item_uuid,
            campaign_uuid,
            business_data_set,
			checksum,
			'HIST' AS table_type
		FROM
			el_eds_def.dimpolicycampaign b
		WHERE
			b.business_key IN (
				SELECT business_key 
				FROM el_eds_def_stg.stgdimpolicycampaign a
				WHERE a.record_eff_from_date <> b.record_eff_from_date
				AND a.source_app_code = 'EBLI'
			)
			AND b.active_record_ind = 'Y'
			AND b.source_app_code = 'EBLI'
			)
		) temp
	) temp;
	
-- Merging the temp table with final table

MERGE INTO el_eds_def.dimpolicycampaign
USING #tempstgdimpolicycampaign temp 
	ON el_eds_def.dimpolicycampaign.business_key = temp.business_key 
	AND el_eds_def.dimpolicycampaign.record_eff_from_date = temp.record_eff_from_date
	AND el_eds_def.dimpolicycampaign.source_app_code = 'EBLI'
WHEN MATCHED
	THEN UPDATE
		SET
			record_updated_date = temp.record_updated_date,
			record_eff_to_date = CASE WHEN 
				el_eds_def.dimpolicycampaign.active_record_ind = 'Y' AND temp.rnk != 1 
				THEN temp.latest_record_eff_from_date 
				ELSE el_eds_def.dimpolicycampaign.record_eff_to_date END,
			active_record_ind = CASE WHEN 
				el_eds_def.dimpolicycampaign.active_record_ind = 'Y' AND temp.rnk != 1 
				THEN temp.active_record_ind 
				ELSE el_eds_def.dimpolicycampaign.active_record_ind END,
                campaign_id= temp.campaign_id,
                campaign_code = temp.campaign_code,
                campaign_name= temp.campaign_name,
                premium_discount_percentage = temp.premium_discount_percentage,
                premium_discount_value = temp.premium_discount_value,
                discount_type  = temp.discount_type,
                policy_no = temp.policy_no,
                policy_id       = temp.policy_id,
                policy_plan_id  = temp.policy_plan_id,
                policy_uuid     = temp.policy_uuid,
                policy_covered_item_uuid  = temp.policy_covered_item_uuid,
                campaign_uuid        = temp.campaign_uuid,
                business_data_set    = temp.business_data_set,
		     	checksum = temp.checksum
WHEN NOT MATCHED
	THEN INSERT (
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
			checksum,
			policy_campaign_uuid,
            business_key,
            campaign_id,
            campaign_code,
            campaign_name,
            premium_discount_percentage,
            premium_discount_value,
            discount_type,
            policy_no,
            policy_id,
            policy_plan_id,
            policy_uuid,
            policy_covered_item_uuid,
            campaign_uuid,
            business_data_set
		)
		VALUES (
			temp.source_app_code,
			temp.source_data_set,
			temp.dml_ind,
			temp.record_created_date,
			temp.record_updated_date,
			temp.record_created_by,
			temp.record_updated_by,
			temp.record_eff_from_date,
			temp.record_eff_to_date,
			temp.active_record_ind,
			temp.checksum,
			temp.policy_campaign_uuid,
            temp.business_key,
            temp.campaign_id,
            temp.campaign_code,
            temp.campaign_name,
            temp.premium_discount_percentage,
            temp.premium_discount_value,
            temp.discount_type,
            temp.policy_no,
            temp.policy_id,
            temp.policy_plan_id,
            temp.policy_uuid,
            temp.policy_covered_item_uuid,
            temp.campaign_uuid,
            temp.business_data_set
		);

DROP TABLE IF EXISTS #tempstgdimpolicycampaign;
END;