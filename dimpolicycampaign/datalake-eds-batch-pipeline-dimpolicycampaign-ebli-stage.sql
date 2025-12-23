/*
Amendment History:
CRQ                     Modified date           modified by       	  description
CRQ000000238343          16-07-2025                Anurag             Update join to dimcustomer to handle spaces
Jira-CRDTLK-73       	 19-Nov-2025     	       Vignesh        	  set 'PRIMARY' for insured_type column
*/
BEGIN;
SET TIMEZONE = 'Singapore';
-- Set last run date for incremental load

CREATE TABLE #v_rundate_cam AS
SELECT 
	NVL(
		CAST(DATE_TRUNC('day', DATEADD(day, -1, src_record_eff_from_date)) AS TIMESTAMP), 
		CAST('1900-01-01 00:00:00.000' AS TIMESTAMP)
	) AS v_vLastRunDate 
FROM (
	SELECT MAX(src_record_eff_from_date) AS src_record_eff_from_date
	FROM el_eds_master_stg.ctrl_audit
	WHERE tgt_table_name = 'dimpolicycampaign'
	AND tgt_source_app_code = 'EBLI' and tgt_source_data_set ='EBLI-CAM'
	);
	
CREATE TABLE #v_rundate_prom AS
SELECT 
	NVL(
		CAST(DATE_TRUNC('day', DATEADD(day, -1, src_record_eff_from_date)) AS TIMESTAMP), 
		CAST('1900-01-01 00:00:00.000' AS TIMESTAMP)
	) AS v_vLastRunDate 
FROM (
	SELECT MAX(src_record_eff_from_date) AS src_record_eff_from_date
	FROM el_eds_master_stg.ctrl_audit
	WHERE tgt_table_name = 'dimpolicycampaign'
	AND tgt_source_app_code = 'EBLI' and tgt_source_data_set ='EBLI-PROMO'
	);


-- Insert dummy record for first load
INSERT INTO el_eds_def.dimpolicycampaign (
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
	business_key
	)
SELECT
	'MANUAL' AS source_app_code,
	'MANUAL' AS source_data_set,
	'I' AS dml_ind,
	GETDATE() AS record_created_date,
	GETDATE() AS record_updated_date,
	'EDS' AS record_created_by,
	'EDS' AS record_updated_by,
	CAST('1900-01-01 00:00:00.000000' AS timestamp) AS record_eff_from_date,
	CAST('9999-12-31 00:00:00.000000' AS timestamp) AS record_eff_to_date,
	'Y' AS active_record_ind,
	-1 AS policy_campaign_uuid,
	-1 AS business_key
WHERE (
	SELECT COUNT(1) FROM el_eds_def.dimpolicycampaign WHERE policy_campaign_uuid = -1
	) = 0;



-- CREATING TEMP TABLES FOR EDS TABLES


CREATE TABLE #tb_t_contract_product_hist AS 
SELECT 
	item_id,
	active_record_ind,
	record_eff_from_date,
	dml_ind,
	policy_id 
FROM 
	(SELECT
			item_id,
	active_record_ind,
	record_eff_from_date,
	dml_ind,
	policy_id ,
		ROW_NUMBER() OVER (
			PARTITION BY 
				business_key 
			ORDER BY 
				COALESCE(change_seq, -1) DESC,
				record_eff_from_date DESC, 
				record_eff_to_date DESC
		) rnk
	FROM
		tl_ebli_def.tb_t_contract_product_hist)
WHERE 
	rnk = 1;
	
CREATE TABLE #tb_t_contract_master_hist AS 
SELECT 
	record_eff_from_date,
	dml_ind,
	active_record_ind,
	campaign_code,
	policy_code,
	policy_id,
	policy_cate 
FROM 
	(SELECT
		record_eff_from_date,
	    dml_ind,
	    active_record_ind,
	    campaign_code,
	    policy_code,
	    policy_id,
	    policy_cate ,
		ROW_NUMBER() OVER (
			PARTITION BY 
				business_key 
			ORDER BY 
				COALESCE(change_seq, -1) DESC,
				record_eff_from_date DESC, 
				record_eff_to_date DESC
		) rnk
	FROM
		tl_ebli_def.tb_t_contract_master_hist)
WHERE 
	rnk = 1;
	


CREATE TABLE #tb_t_campaign_info_hist AS 
SELECT 
	campaign_name ,
	campaign_code ,
	record_eff_from_date,
	dml_ind,
	active_record_ind,
	record_eff_to_date,
	prem_discount
FROM 
	(SELECT
	campaign_name ,
	campaign_code ,
	record_eff_from_date,
	dml_ind,
	active_record_ind,
	record_eff_to_date,
	prem_discount,
		ROW_NUMBER() OVER (
			PARTITION BY 
				business_key 
			ORDER BY 
				COALESCE(change_seq, -1) DESC,
				record_eff_from_date DESC, 
				record_eff_to_date DESC
		) rnk
	FROM
		tl_ebli_def.tb_t_campaign_info_hist)
WHERE 
	rnk = 1;	
	

CREATE TABLE #tb_t_product_promotion_hist AS 
SELECT 
	item_id ,
	campaign_code,
	dml_ind,
	active_record_ind,
	record_eff_from_date
FROM 
	(SELECT
	item_id ,
	campaign_code,
	dml_ind,
	active_record_ind,
	record_eff_from_date,
		ROW_NUMBER() OVER (
			PARTITION BY 
				business_key 
			ORDER BY 
				COALESCE(change_seq, -1) DESC,
				record_eff_from_date DESC, 
				record_eff_to_date DESC
		) rnk
	FROM
		tl_ebli_def.tb_t_product_promotion_hist)
WHERE 
	rnk = 1;	

	

CREATE TABLE #dimpolicycovereditem  AS 
SELECT 
	policy_id ,
	source_data_set ,
	source_app_code,
	policy_uuid,
	policy_covered_item_uuid,
	covered_item_id 
FROM 
	el_eds_def.dimpolicycovereditem  
WHERE
	active_record_ind = 'Y'
	AND source_app_code = 'EBLI';
	

CREATE TABLE #dimpolicy  AS 
SELECT 
	policy_id ,
	source_data_set,
	source_app_code,
	policy_uuid
FROM 
	el_eds_def.dimpolicy  
WHERE
	active_record_ind = 'Y'
	AND source_app_code = 'EBLI';
	
	
	

 
-- PKPrimary staging - driver tables for promotions
CREATE TABLE #PKPrimaryPROM_stg_driver AS 
SELECT 
	policy_id, item_id, campaign_code, record_eff_from_date, dml_ind,active_record_ind
FROM (
	SELECT pg.policy_id, cp.item_id, tcpp.campaign_code, pg.record_eff_from_date, pg.dml_ind,pg.active_record_ind
	FROM #tb_t_contract_master_hist pg
	INNER JOIN #tb_t_contract_product_hist cp ON cp.policy_id = pg.policy_id
	INNER JOIN #tb_t_product_promotion_hist tcpp ON tcpp.item_id = cp.item_id
	UNION
	SELECT pg.policy_id, cp.item_id, tcpp.campaign_code, cp.record_eff_from_date, cp.dml_ind,cp.active_record_ind
	FROM #tb_t_contract_product_hist cp
	INNER JOIN #tb_t_contract_master_hist pg ON cp.policy_id = pg.policy_id
	INNER JOIN #tb_t_product_promotion_hist tcpp ON tcpp.item_id = cp.item_id
);
 
-- PKPrimary driver for promotions
CREATE TABLE #PKPrimaryPROM_driver AS
SELECT 
	policy_id, item_id as item_id, campaign_code, dml_ind,active_record_ind
FROM (
	SELECT
		policy_id, item_id, campaign_code, dml_ind,active_record_ind,
		ROW_NUMBER() OVER (
			PARTITION BY policy_id, item_id, campaign_code
			ORDER BY CASE WHEN dml_ind = 'D' THEN 1 ELSE 2 END,
			record_eff_from_date DESC
		) rnk
	FROM #PKPrimaryPROM_stg_driver
)
WHERE rnk = 1;
 
-- PKPrimary staging - incremental load
CREATE TABLE #PKPrimarycam_stg AS 
SELECT 
	policy_id, campaign_code, record_eff_from_date
FROM (
	SELECT policy_id, campaign_code, record_eff_from_date
	FROM #tb_t_contract_master_hist
	WHERE record_eff_from_date >= (SELECT v_vLastRunDate FROM #v_rundate_cam)
	UNION
	SELECT pg.policy_id, pg.campaign_code, tci.record_eff_from_date
	FROM #tb_t_contract_master_hist pg
	INNER JOIN #tb_t_campaign_info_hist tci ON tci.campaign_code = pg.campaign_code
	WHERE tci.record_eff_from_date >= (SELECT v_vLastRunDate FROM #v_rundate_cam)
);

CREATE TABLE #PKPrimarycam_total AS
SELECT 
	policy_id, campaign_code, record_eff_from_date
FROM (
	SELECT
		policy_id, campaign_code, record_eff_from_date,
		ROW_NUMBER() OVER (
			PARTITION BY policy_id, campaign_code 
			ORDER BY record_eff_from_date DESC
		) rnk
	FROM #PKPrimarycam_stg
)
WHERE rnk = 1;
 

CREATE TABLE #PKPrimaryprom_stg AS
SELECT 
	policy_id,item_id, campaign_code, record_eff_from_date
FROM (
	SELECT pg.policy_id,cp.item_id, tcpp.campaign_code, pg.record_eff_from_date
	FROM #tb_t_contract_master_hist pg
	INNER JOIN #tb_t_contract_product_hist cp ON cp.policy_id = pg.policy_id
	INNER JOIN #tb_t_product_promotion_hist tcpp ON tcpp.item_id = cp.item_id
	WHERE pg.record_eff_from_date >= (SELECT v_vLastRunDate FROM #v_rundate_prom)
	UNION
	SELECT pg.policy_id,cp.item_id, tcpp.campaign_code, cp.record_eff_from_date
	FROM #tb_t_contract_product_hist cp
	INNER JOIN #tb_t_contract_master_hist pg ON cp.policy_id = pg.policy_id
	INNER JOIN #tb_t_product_promotion_hist tcpp ON tcpp.item_id = cp.item_id
	WHERE cp.record_eff_from_date >= (SELECT v_vLastRunDate FROM #v_rundate_prom)
	UNION
	SELECT pg.policy_id,cp.item_id, tcpp.campaign_code, tcpp.record_eff_from_date
	FROM #tb_t_contract_product_hist cp
	INNER JOIN #tb_t_contract_master_hist pg ON cp.policy_id = pg.policy_id
	INNER JOIN #tb_t_product_promotion_hist tcpp ON tcpp.item_id = cp.item_id
	WHERE tcpp.record_eff_from_date >= (SELECT v_vLastRunDate FROM #v_rundate_prom)
	UNION
	SELECT pg.policy_id,cp.item_id, tcpp.campaign_code, tci.record_eff_from_date
	FROM #tb_t_contract_product_hist cp
	INNER JOIN #tb_t_contract_master_hist pg ON cp.policy_id = pg.policy_id
	INNER JOIN #tb_t_product_promotion_hist tcpp ON tcpp.item_id = cp.item_id
	INNER JOIN #tb_t_campaign_info_hist tci on tci.campaign_code = pg.campaign_code
	WHERE tci.record_eff_from_date >= (SELECT v_vLastRunDate FROM #v_rundate_prom)
);
 
-- PKPrimary total
CREATE TABLE #PKPrimaryprom_total AS
SELECT 
	policy_id,item_id,campaign_code, record_eff_from_date
FROM (
	SELECT
		policy_id, item_id,campaign_code, record_eff_from_date,
		ROW_NUMBER() OVER (
			PARTITION BY policy_id, item_id,campaign_code 
			ORDER BY record_eff_from_date DESC
		) rnk
	FROM #PKPrimaryprom_stg
)
WHERE rnk = 1;
 
 
-- PKPrimary final for promotions  
CREATE TABLE #PKPrimaryPROM AS
SELECT 
	a.policy_id, a.item_id,a.campaign_code, a.record_eff_from_date,
	case when b.dml_ind<>'D' then 'I' else b.dml_ind end as dml_ind
FROM 
	#PKPrimaryprom_total a 
	INNER JOIN #PKPrimaryPROM_driver b 
	ON a.policy_id = b.policy_id and a.item_id=b.item_id AND a.campaign_code = b.campaign_code
;



create table #dimpolicycampaign AS
select
dml_ind,
checksum,
active_record_ind,
business_key
from(select
dml_ind,
checksum,
active_record_ind,
business_key,
row_number()over(partition by business_key order by  record_eff_from_date desc,record_eff_to_date desc ) rnk
from el_eds_def.dimpolicycampaign where source_app_code='EBLI')
where rnk=1;


create table  #tempdimpolicycampaign as 

	SELECT
		'EBLI' AS source_app_code,
		'EBLI-CAM' AS source_data_set,
		case when pg.dml_ind<>'D' then 'I' else pg.dml_ind end as dml_ind,
		pk.record_eff_from_date,
		SHA2('EBLI~' || pg.policy_id  || '~' || pg.campaign_code, 256) AS policy_campaign_uuid,
		'EBLI~' || pg.policy_id || '~' || pg.campaign_code AS business_key,
		pg.campaign_code AS campaign_id,
		tci.campaign_code as campaign_code,
		tci.campaign_name AS campaign_name,
		(1 - tci.prem_discount) AS premium_discount_percentage,
		NULL AS premium_discount_value,
		'Staff Discount' as discount_type,
		pg.policy_code as policy_no,
		pg.policy_id AS policy_id,
		NULL AS policy_plan_id,
		isnull(pol.policy_uuid,'-1') as policy_uuid,
		NULL AS policy_covered_item_uuid,
		('EBLI~' || pg.campaign_code) as campaign_uuid,
		'EBLI_IndividualLife_CampaignCode' as business_data_set
	FROM 
		#PKPrimarycam_total pk
         INNER JOIN  #tb_t_contract_master_hist pg ON pk.policy_id =  pg.policy_id and pk.campaign_code = pg.campaign_code
		 LEFT OUTER JOIN #tb_t_campaign_info_hist tci ON tci.campaign_code = pg.campaign_code and tci.active_record_ind ='Y'
         LEFT OUTER JOIN #dimpolicy pol ON pol.policy_id = pg.policy_id
         where pg.policy_cate not in ('2','3','4') 
		 and pg.campaign_code is not null	
	UNION 
	SELECT
		'EBLI' AS source_app_code,
		'EBLI-PROMO' AS source_data_set,
		pkr.dml_ind,
		pkr.record_eff_from_date,
		SHA2('EBLI~' || pg.policy_id || '~' || cp.item_id || coalesce('~' || tcpp.campaign_code,''), 256) AS policy_campaign_uuid,
		'EBLI~' || pg.policy_id || '~' || cp.item_id || coalesce('~' || tcpp.campaign_code,'') AS business_key,
		tcpp.campaign_code AS campaign_id,
		tcpp.campaign_code as campaign_code,
		tci.campaign_name AS campaign_name,
		(1 - tci.prem_discount) AS premium_discount_percentage,
		NULL AS premium_discount_value,
		'Voucher & Gift' as discount_type,
		pg.policy_code as policy_no,
		pg.policy_id AS policy_id,
		cp.item_id AS policy_plan_id,
		isnull(pol.policy_uuid,'-1') as policy_uuid,
		isnull(pol.policy_covered_item_uuid,'-1')  AS policy_covered_item_uuid,
		('EBLI~' || tcpp.campaign_code) as campaign_uuid,
		'EBLI_IndividualLife_PromotionCode' as business_data_set
	FROM 
		#PKPrimaryPROM pkr
		inner join  #tb_t_contract_master_hist pg ON pkr.policy_id =  pg.policy_id
inner join #tb_t_contract_product_hist cp on cp.policy_id = pg.policy_id and pkr.item_id = cp.item_id
inner join #tb_t_product_promotion_hist t on t.campaign_code = pkr.campaign_code and t.item_id=cp.item_id and t.campaign_code <> pg.campaign_code 
and t.campaign_code is not null
left join #tb_t_product_promotion_hist tcpp on tcpp.item_id = cp.item_id and tcpp.campaign_code is not null
and tcpp.campaign_code <> pg.campaign_code and  tcpp.active_record_ind='Y'
left join #tb_t_campaign_info_hist tci on tci.campaign_code = tcpp.campaign_code and tci.active_record_ind='Y' 
left outer join #dimpolicycovereditem pol on pol.policy_id = pg.policy_id and pol.covered_item_id = cp.item_id
where pg.policy_cate not in ('2','3','4');

	
UPDATE #tempdimpolicycampaign
		SET dml_ind= case when a.dml_ind<>'D' then 'U' else a.dml_ind end
		FROM #tempdimpolicycampaign a inner join #dimpolicycampaign b  on  a.business_key = b.business_key where b.active_record_ind='Y';





-- Temp table with hashed checksum column
CREATE TABLE #tempdimpolicycampaignHash AS
SELECT
	*,
	SHA2(
		COALESCE(CAST(source_app_code AS varchar), CAST('null' AS varchar)) +
		COALESCE(CAST(source_data_set AS varchar), CAST('null' AS varchar)) +
		COALESCE(CAST(policy_campaign_uuid AS varchar), CAST('null' AS varchar)) +
		COALESCE(CAST(business_key  AS varchar), CAST('null' AS varchar)) +
		COALESCE(CAST(campaign_id AS varchar), CAST('null' AS varchar)) +
		COALESCE(CAST(campaign_code AS varchar), CAST('null' AS varchar)) +
		COALESCE(CAST(campaign_name AS varchar), CAST('null' AS varchar)) +
		COALESCE(CAST(premium_discount_percentage AS varchar), CAST('null' AS varchar)) +
		COALESCE(CAST(premium_discount_value AS varchar), CAST('null' AS varchar)) +
		COALESCE(CAST(discount_type AS varchar), CAST('null' AS varchar)) +
		COALESCE(CAST(policy_no AS varchar), CAST('null' AS varchar)) +
		COALESCE(CAST(policy_id AS varchar), CAST('null' AS varchar)) +
		COALESCE(CAST(policy_plan_id AS varchar), CAST('null' AS varchar)) +
		COALESCE(CAST(policy_uuid AS varchar), CAST('null' AS varchar)) +
		COALESCE(CAST(policy_covered_item_uuid AS varchar), CAST('null' AS varchar)) +
		COALESCE(CAST(campaign_uuid AS varchar), CAST('null' AS varchar))+
		COALESCE(CAST(business_data_set AS varchar), CAST('null' AS varchar))
	, 256) AS checksum
FROM
	#tempdimpolicycampaign;



-- Final temp table to get distinct records
CREATE TABLE #tempdimpolicycampaignStgFinal AS
select * from (select a.* , case when a.dml_ind='D' and b.dml_ind<>'D' then 1 when a.dml_ind in('I','U') AND b.dml_ind='D' THEN 1 when a.checksum <> coalesce(b.checksum,'1') and coalesce(b.active_record_ind,'Y')='Y' then 1 else 0 end as changed_rec_check from #tempdimpolicycampaignHash a left outer join #dimpolicycampaign b on a.business_key = b.business_key ) where changed_rec_check =1;

-- Truncate and insert into staging table
DELETE FROM el_eds_def_stg.stgdimpolicycampaign WHERE source_app_code = 'EBLI';

INSERT INTO el_eds_def_stg.stgdimpolicycampaign (
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
	SELECT
		source_app_code,
		source_data_set,
		dml_ind,
		GETDATE() AS record_created_date,
		GETDATE() AS record_updated_date,
		'EDS' AS record_created_by,
		'EDS' AS record_updated_by,
		record_eff_from_date,
		CAST('9999-12-31 00:00:00.000000' AS timestamp) AS record_eff_to_date,
		'Y' AS active_record_ind,
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
	FROM #tempdimpolicycampaignStgFinal
	;
	

-----AUDIT-----
create table #min_record_eff_from_date as
select
  min(record_eff_from_date) AS min_record_eff_from_date,
  'EBLI-CAM'::VARCHAR(50) AS source_data_set, 
  'EBLI'::VARCHAR(50) AS source_app_code
from
  (
    SELECT MAX(record_eff_from_date) AS record_eff_from_date FROM tl_ebli_def.tb_t_contract_master_hist WHERE active_record_ind = 'Y'
UNION   
SELECT MAX(record_eff_from_date) AS record_eff_from_date FROM tl_ebli_def.tb_t_campaign_info_hist WHERE active_record_ind = 'Y');


insert into #min_record_eff_from_date 
select
  min(record_eff_from_date) as min_record_eff_from_date,
  'EBLI-PROMO' AS source_data_set, 'EBLI' AS source_app_code
from
  (
    SELECT MAX(record_eff_from_date) AS record_eff_from_date FROM tl_ebli_def.tb_t_contract_master_hist WHERE active_record_ind = 'Y'
UNION   
SELECT MAX(record_eff_from_date) AS record_eff_from_date FROM tl_ebli_def.tb_t_contract_product_hist WHERE active_record_ind = 'Y'
UNION 
    SELECT MAX(record_eff_from_date) AS record_eff_from_date FROM tl_ebli_def.tb_t_product_promotion_hist WHERE active_record_ind = 'Y'
UNION   
SELECT MAX(record_eff_from_date) AS record_eff_from_date FROM tl_ebli_def.tb_t_campaign_info_hist WHERE active_record_ind = 'Y'
  );
  
DELETE FROM el_eds_def_stg.ctrl_audit_stg where tgt_table_name= 'dimpolicycampaign' and tgt_source_app_code='EBLI';


Insert Into el_eds_def_stg.ctrl_audit_stg
select
  source_data_set as tgt_source_data_set,
  source_app_code tgt_source_app_code,
  'el_eds_def' as tgt_schema,
  'dimpolicycampaign' as tgt_table_name,
  min_record_eff_from_date as src_record_eff_from_date,
  Null as tgt_record_eff_from_date,
  getdate() as data_pipeline_run_date,
  getdate() as record_created_date,
  getdate() as record_updated_date
from #min_record_eff_from_date;

-----------------------------------------


create table #src_count as(
select
  count(1) as source_count, 'tl_ebli_def' as src_schema, 'tb_t_contract_master_hist'::VARCHAR(1000) as src_table, 'EBLI-CAM'::VARCHAR(50) as entity
from
  (
   SELECT DISTINCT
			pg.policy_id, pg.campaign_code, pg.active_record_ind
		FROM  #tb_t_contract_master_hist pg
		 where pg.policy_cate not in ('2','3','4') 
		 and pg.campaign_code is not null	
  )
where
  active_record_ind = 'Y'
);


insert Into #src_count (
select
  count(1) as source_count, 'tl_ebli_def' as src_schema, 'tb_t_contract_master_hist' as src_table, 'EBLI-PROMO' as entity
from
  (
   SELECT DISTINCT
			'EBLI-PROMO' AS source_data_set, pkr.policy_id, pkr.item_id,pkr.campaign_code, pkr.active_record_ind
		FROM
			#PKPrimaryPROM_driver pkr
			inner join  #tb_t_contract_master_hist pg ON pkr.policy_id =  pg.policy_id 
            inner join #tb_t_contract_product_hist cp on cp.policy_id = pg.policy_id and pkr.item_id = cp.item_id 
			inner join #tb_t_product_promotion_hist t on t.item_id = cp.item_id and pkr.campaign_code = t.campaign_code and t.campaign_code <> pg.campaign_code 
and t.campaign_code is not null where pg.policy_cate not in ('2','3','4') 
  )
where
  active_record_ind = 'Y'
);


DELETE FROM el_eds_def_stg.dq_audit_stg WHERE entity in (select entity from #src_count) and tgt_table ='dimpolicycampaign';


insert into
  el_eds_def_stg.dq_audit_stg(
    select
      'eds' as app_name,
      src_schema,
      src_table,
      'el_eds_def' as tgt_schema,
      'dimpolicycampaign' as tgt_table,
      entity,
      '*' as instance,
      'count' as check_type,
      source_count
	from #src_count);

DROP TABLE IF EXISTS #v_rundate_cam;
DROP TABLE IF EXISTS #v_rundate_prom;
DROP TABLE IF EXISTS #tb_t_contract_product_hist ;
DROP TABLE IF EXISTS #tb_t_contract_master_hist;
DROP TABLE IF EXISTS #tb_t_campaign_info_hist;
DROP TABLE IF EXISTS #tb_t_product_promotion_hist;
DROP TABLE IF EXISTS #dimpolicycovereditem;
DROP TABLE IF EXISTS #dimpolicy;
DROP TABLE IF EXISTS #PKPrimaryPROM_stg_driver;
DROP TABLE IF EXISTS #PKPrimaryPROM_driver;
DROP TABLE IF EXISTS #PKPrimarycam_stg;
DROP TABLE IF EXISTS #PKPrimarycam_total;
DROP TABLE IF EXISTS #PKPrimaryprom_stg;
DROP TABLE IF EXISTS #PKPrimaryprom_total;
DROP TABLE IF EXISTS #PKPrimaryPROM;
DROP TABLE IF EXISTS #dimpolicycampaign;
DROP TABLE IF EXISTS #tempdimpolicycampaign;
DROP TABLE IF EXISTS #tempdimpolicycampaignHash;
DROP TABLE IF EXISTS #tempdimpolicycampaignStgFinal;
DROP TABLE IF EXISTS #min_record_eff_from_date;
DROP TABLE IF EXISTS #src_count ;

END;