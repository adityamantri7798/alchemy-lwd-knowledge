BEGIN;
SET TIMEZONE = 'Singapore';

INSERT INTO el_eds_def.factpolicypayout (
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
	benefit_payout_uuid,
	business_key
	)
SELECT
	'MANUAL',
	'MANUAL',
	'I' AS dml_ind,
	GETDATE() AS record_created_date,
	GETDATE() AS record_updated_date,
	'EDS' AS record_created_by,
	'EDS' AS record_updated_by,
	CAST('1900-01-01 00:00:00.000000' AS timestamp) AS record_eff_from_date,
	CAST('9999-12-31 00:00:00.000000' AS timestamp) AS record_eff_to_date,
	'Y' AS active_record_ind,
	-1 AS benefit_payout_uuid,
	('MANUAL' || '~' || -1) AS business_key
WHERE (
	SELECT COUNT(1) FROM el_eds_def.factpolicypayout WHERE benefit_payout_uuid = -1
	) = 0;
  
CREATE TABLE #v_rundate AS
SELECT nvl(CAST(DATE_TRUNC('day',DATEADD (day,-1,src_record_eff_from_date)) AS TIMESTAMP),CAST('1900-01-01 00:00:00.000' AS TIMESTAMP)) AS v_vLastRunDate
FROM (SELECT MAX(src_record_eff_from_date) AS src_record_eff_from_date
      FROM el_eds_def_stg.ctrl_audit 
	  WHERE
      tgt_table_name='factpolicypayout' and tgt_source_app_code='EBLI');

--creating temp tables for both eds and tds tables	
create table #tb_t_pay_due_hist as
select dml_ind
,active_record_ind
,record_eff_from_date
,source_app_code
,policy_id
,item_id
,pay_id
,fee_amount
,pay_num
,pay_due_date
from (select 
dml_ind
,active_record_ind
,source_app_code
,record_eff_from_date
,policy_id
,item_id
,pay_id
,fee_amount
,pay_num
,pay_due_date
,row_number() over(partition by business_key order by coalesce(change_seq,-1) desc, record_eff_from_date desc, record_eff_to_date desc) as rnk
FROM tl_ebli_def.tb_t_pay_due_hist)
 WHERE rnk=1;

create table #tb_t_contract_master_hist as
select 
policy_code
,policy_id
,policy_cate
,record_eff_from_date
,dml_ind
,active_record_ind
from (select 
policy_code
,policy_id
,policy_cate
,record_eff_from_date
,dml_ind
,active_record_ind
,row_number() over(partition by business_key order by coalesce(change_seq,-1) desc,record_eff_from_date desc, record_eff_to_date desc) as rnk 
from tl_ebli_def.tb_t_contract_master_hist)
WHERE rnk=1;


CREATE TABLE #dimpolicycovereditem  AS 
SELECT 
	policy_id ,
	policy_uuid,
	source_data_set ,
	source_app_code,
	covered_item_id,
	policy_covered_item_uuid 
FROM 
	el_eds_def.dimpolicycovereditem  
WHERE
	active_record_ind = 'Y'
	AND source_app_code = 'EBLI';



CREATE TABLE #PKPrimary_stg AS 
SELECT policy_id,pay_id,record_eff_from_date
FROM
(
		SELECT 
			pay.policy_id,pay.pay_id,pay.record_eff_from_date
		FROM 
		#tb_t_pay_due_hist pay
		WHERE pay.record_eff_from_date >=(SELECT v_vLastRunDate FROM #v_rundate) 
	UNION 
		Select 
			pay.policy_id,pay.pay_id, pg.record_eff_from_date
		FROM 
		#tb_t_pay_due_hist pay
		 inner join #tb_t_contract_master_hist pg
        ON pg.policy_id  = pay.policy_id 
		WHERE pg.record_eff_from_date >=(SELECT v_vLastRunDate FROM #v_rundate) 
	
);

CREATE TABLE #PKPrimary AS
Select policy_id,pay_id,record_eff_from_date
FROM(Select
policy_id,
pay_id,
record_eff_from_date,
row_number() over( partition by policy_id,pay_id order by record_eff_from_date desc ) rnk
from #PKPrimary_stg )
where rnk=1;


create table #factpolicypayout AS
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
from el_eds_def.factpolicypayout where source_app_code='EBLI')
where rnk=1;


CREATE TABLE #tempstgfactpolicypayout as 
SELECT
	    'EBLI' as source_app_code,
		'EBLI' as source_data_set,
		case when pay.dml_ind<>'D' then 'I' else pay.dml_ind end as dml_ind,
		pkg.record_eff_from_date,
       sha2('EBLI' || '~' || cast(pay.policy_id as varchar) || '~' || cast(pay.pay_id as varchar)
	   ,256) as benefit_payout_uuid,
        ('EBLI' || '~' || cast(pay.policy_id as varchar) || '~' || cast(pay.pay_id as varchar)) as business_key,
		pg.policy_code as policy_no,
        pay.policy_id as policy_id,
        pay.item_id as policy_plan_id,
        pay.pay_id as benefit_payout_id,
        pay.fee_amount as benefit_payout_amount,
        pay.pay_num as benefit_payout_order,
        pay.pay_due_date as benefit_payment_date,
		pcov.policy_uuid as policy_uuid,
        pcov.policy_covered_item_uuid as policy_covered_item_uuid,
		'EBLI_IndividualLife' as business_data_set
	FROM #PKPrimary pkg 
	inner join #tb_t_pay_due_hist pay on pkg.policy_id= pay.policy_id and pkg.pay_id= pay.pay_id 
    LEFT JOIN #tb_t_contract_master_hist pg on pg.policy_id = pay.policy_id
	and pg.policy_cate not in ('2','3','4') and pg.active_record_ind='Y'
	LEFT JOIN #dimpolicycovereditem pcov on pcov.policy_id = pay.policy_id 
    and pcov.covered_item_id = pay.item_id;

	
UPDATE #tempstgfactpolicypayout
		SET dml_ind= case when a.dml_ind<>'D' then 'U' else a.dml_ind end
		FROM #tempstgfactpolicypayout a inner join #factpolicypayout b on  a.business_key = b.business_key where b.active_record_ind='Y';


--creating sha2 to compare the records from staging to final
create table #hashstgfactpolicypayout as
SELECT 
            source_app_code
            ,source_data_set
            ,dml_ind
            ,record_eff_from_date
            ,benefit_payout_uuid
            ,business_key
            ,policy_no
            ,policy_id
            ,policy_plan_id
            ,benefit_payout_id
            ,benefit_payout_amount
            ,benefit_payout_order
            ,benefit_payment_date
            ,policy_uuid
            ,policy_covered_item_uuid
			,business_data_set
			,sha2(
       coalesce(cast(source_app_code as varchar),cast('null' as varchar))+
coalesce(cast(source_data_set as varchar),cast('null' as varchar))+
coalesce(cast(benefit_payout_uuid as varchar),cast('null' as varchar))+
coalesce(cast(business_key as varchar),cast('null' as varchar))+
coalesce(cast(policy_no as varchar),cast('null' as varchar))+
coalesce(cast(policy_id as varchar),cast('null' as varchar))+
coalesce(cast(policy_plan_id as varchar),cast('null' as varchar))+
coalesce(cast(benefit_payout_id as varchar),cast('null' as varchar))+
coalesce(cast(benefit_payout_amount as varchar),cast('null' as varchar))+
coalesce(cast(benefit_payout_order as varchar),cast('null' as varchar))+
coalesce(cast(benefit_payment_date as varchar),cast('null' as varchar))+
coalesce(cast(policy_uuid as varchar),cast('null' as varchar))+
coalesce(cast(policy_covered_item_uuid as varchar),cast('null' as varchar))+
coalesce(cast(business_data_set as varchar),cast('null' as varchar)),256) 
as checksum 
from #tempstgfactpolicypayout 
;


create table #stgfactpolicypayout AS
select * from (select a.* , case when a.dml_ind='D' and b.dml_ind<>'D' then 1 when a.dml_ind in('I','U') AND b.dml_ind='D' THEN 1 when a.checksum <> coalesce(b.checksum,'1') and coalesce(b.active_record_ind,'Y')='Y' then 1 else 0 end as changed_rec_check from #hashstgfactpolicypayout a left outer join #factpolicypayout b on a.business_key = b.business_key ) where changed_rec_check =1;





DELETE FROM el_eds_def_stg.stgfactpolicypayout WHERE source_app_code='EBLI';
		
INSERT INTO el_eds_def_stg.stgfactpolicypayout 
(
           source_app_code
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
           ,benefit_payout_uuid
           ,business_key
           ,policy_no
           ,policy_id
           ,policy_plan_id
           ,benefit_payout_id
           ,benefit_payout_amount
           ,benefit_payout_order
           ,benefit_payment_date
           ,policy_uuid
           ,policy_covered_item_uuid
		   ,business_data_set)
select
           source_app_code
           ,source_data_set
           ,dml_ind
           ,getdate() as record_created_date
           ,getdate() as record_updated_date
           ,'EDS' as record_created_by 
           ,'EDS' as record_updated_by 
           ,record_eff_from_date
           ,cast('9999-12-31 00:00:00.000000' as timestamp) as record_eff_to_date
           ,'Y' as active_record_ind
           ,checksum
           ,benefit_payout_uuid
           ,business_key
           ,policy_no
           ,policy_id
           ,policy_plan_id
           ,benefit_payout_id
           ,benefit_payout_amount
           ,benefit_payout_order
           ,benefit_payment_date
           ,policy_uuid
           ,policy_covered_item_uuid 
		   ,business_data_set
from #stgfactpolicypayout;		




---------AUDIT--------------------

create table #min_record_eff_from_date as
select
  min(record_eff_from_date) AS min_record_eff_from_date ,'EBLI' AS source_data_set , 'EBLI' AS source_app_code
from	
(
select max(record_eff_from_date) as record_eff_from_date from tl_ebli_def.tb_t_pay_due_hist where active_record_ind='Y'
UNION
select max(record_eff_from_date) as record_eff_from_date from tl_ebli_def.tb_t_contract_master_hist where active_record_ind='Y'
);


DELETE FROM el_eds_def_stg.ctrl_audit_stg where tgt_table_name= 'factpolicypayout' and tgt_source_app_code='EBLI';

Insert Into el_eds_def_stg.ctrl_audit_stg
select
  source_data_set as tgt_source_data_set,
  source_app_code tgt_source_app_code,
  'el_eds_def' as tgt_schema,
  'factpolicypayout' as tgt_table_name,
  min_record_eff_from_date as src_record_eff_from_date,
  Null as tgt_record_eff_from_date,
  getdate() as data_pipeline_run_date,
  getdate() as record_created_date,
  getdate() as record_updated_date
from #min_record_eff_from_date
;

-------------dq_audit----------------------------

create table #src_count as(
select
  count(1) as source_count, 'tl_ebli_def' as src_schema, 'tb_t_pay_due_hist' as src_table, 'EBLI' as entity
from(
 SELECT DISTINCT 
        pay.policy_id,pay.pay_id,
        pay.active_record_ind AS active_record_ind
    FROM  #tb_t_pay_due_hist pay 
)
where active_record_ind='Y');


DELETE FROM el_eds_def_stg.dq_audit_stg WHERE entity in (select entity from #src_count) and tgt_table ='factpolicypayout';

insert into
  el_eds_def_stg.dq_audit_stg(
    select
      'eds' as app_name,
      src_schema,
      src_table,
      'el_eds_def' as tgt_schema,
      'factpolicypayout' as tgt_table,
      entity,
      '*' as instance,
      'count' as check_type,
      source_count
	from #src_count);
	
	
DROP TABLE IF EXISTS #v_rundate;
DROP TABLE IF EXISTS #tb_t_pay_due_hist;
DROP TABLE IF EXISTS #tb_t_contract_master_hist;
DROP TABLE IF EXISTS #tb_t_contract_product_hist;
DROP TABLE IF EXISTS #PKPrimary_stg_driver; 
DROP TABLE IF EXISTS #PKPrimary_driver;
DROP TABLE IF EXISTS #PKPrimary_stg;
DROP TABLE IF EXISTS #PKPrimary_total;
DROP TABLE IF EXISTS #PKPrimary;
DROP TABLE IF EXISTS #tempstgfactpolicypayout; 
DROP TABLE IF EXISTS #hashstgfactpolicypayout;
DROP TABLE IF EXISTS #stgfactpolicypayout;
DROP TABLE IF EXISTS #factpolicypayout;
DROP TABLE #src_count;
DROP TABLE #min_record_eff_from_date;
END;
	