BEGIN;
SET TIMEZONE = 'Singapore';	

INSERT INTO el_eds_def.factpolicycovereditem (
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
	policy_covered_item_uuid,
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
	-1 AS policy_covered_item_uuid,
	('MANUAL' || '~' || -1) AS business_key
WHERE (
	SELECT COUNT(1) FROM el_eds_def.factpolicycovereditem WHERE policy_covered_item_uuid = -1
	) = 0;

CREATE TABLE #dimdate as
select date_key from el_eds_def.dimdate;

CREATE TABLE #dimpolicycovereditem as
select policy_covered_item_uuid,policy_uuid,policy_id,covered_item_id,submit_date,entry_date,commencement_date,issue_date,start_date,end_date,scan_date,sales_agent_uuid,source_app_code,record_eff_from_date from (select policy_covered_item_uuid,policy_uuid,policy_id,covered_item_id,submit_date,entry_date,commencement_date,issue_date,start_date,end_date,scan_date,sales_agent_uuid,source_app_code,record_eff_from_date,row_number() over(partition by business_key order by record_eff_from_date desc) as rnk from el_eds_def.dimpolicycovereditem where source_app_code='ISIS') WHERE rnk=1;

CREATE TABLE #tempstgdimpolicycovereditem AS
select source_app_code,source_data_set,dml_ind,record_eff_from_date,business_key,policy_uuid,covered_item_id,policy_id,policy_no,campaign_uuid,product_uuid,status_id,submit_date,commencement_date,issue_date,start_date,end_date,nett_premium,gst_on_premium,total_premium,annualised_premium_discount_without_gst,gst_on_discount_amount,loading_amount,gst_on_loading_amount,spi,annualised_premium_payable_without_gst,wpi,sum_assured,sales_agent_uuid,servicing_agent_uuid,scan_date,entry_date,customer_uuid,policy_type_code,business_type_code,source_code,sub_product_uuid,covered_item_type_code,premium_mode_id,pay_mode_id,Retained_Premium_Without_GST,Annualised_Standard_Premium_Without_GST,Annualised_Occupational_Loading,Other_Loading,Annualised_Disability_Loading,Sub_Standard_Rate,Annualised_Mortality_Loading,ipmi_and_rider_discount_without_gst,ipmi_and_rider_discount_gst,ipmi_and_rider_loading_premium_without_gst,ipmi_and_rider_loading_premium_gst,mshl_additional_premium_without_gst,mshl_additional_premium_gst,mshl_premium_without_gst,mshl_premium_gst,ipmi_and_rider_premium_without_gst,ipmi_and_rider_premium_gst,mshl_premium_payable_without_gst,mshl_premium_payable_gst,ipmi_and_rider_premium_payable_without_gst,ipmi_and_rider_premium_payable_gst,mshl_pioneer_generation_subsidies_without_gst,mshl_pioneer_generation_subsidies_gst,mshl_premium_subsidies_for_lower_to_middle_income_households_without_gst,mshl_premium_subsidies_for_lower_to_middle_income_households_gst,mshl_transitional_subsidies_without_gst,mshl_transitional_subsidies_gst,mshl_premium_rebates_without_gst,mshl_premium_rebates_gst,mshl_pensioner_scheme_without_gst,mshl_pensioner_scheme_gst,Cover_Category,ipmi_and_rider_discount_with_gst,ipmi_and_rider_loading_premium_with_gst,mshl_additional_premium_with_gst,mshl_premium_with_gst,ipmi_and_rider_premium_with_gst,mshl_premium_payable_with_gst,ipmi_and_rider_premium_payable_with_gst,mshl_pioneer_generation_subsidies_with_gst,mshl_premium_subsidies_for_lower_to_middle_income_households_with_gst,mshl_transitional_subsidies_with_gst,mshl_premium_rebates_with_gst,mshl_pensioner_scheme_with_gst,master_policy_no,no_of_instalment,insurance_fund_code,Commission,coinsurance_premium,coinsurance_commission,annual_gross_premium,standard_gross_premium,standard_nett_premium,adjusted_net_premium,premium_rebate,si_currency_type,gross_premium,gross_commission,premium_status_id,corp_sales_agent_uuid,cash_benefit,accumulated_bonus,surrender_bonus,non_inforce_policy_status_changed_date,reinstatement_date,standard_premium_without_gst ,loading_premium_without_gst ,premium_payable_with_gst ,premium_payable_without_gst,gst_on_premium_payable,Annualised_Standard_Premium_With_GST,
		Annualised_Premium_Payable_With_GST
		,Annualised_Premium_Discount_With_GST
		,Annualised_Subsidy_Without_GST 
		,Annualised_Subsidy_With_GST
		,Annualised_Premium_Extra_Without_GST 
		,Annualised_Premium_Extra_With_GST
FROM el_eds_def_stg.tempstgdimpolicycovereditem where source_app_code='ISIS';


CREATE TABLE #dimpolicystatusmapping AS 
SELECT 
Policy_Stage
,Policy_Status_Code
,Policy_Status_Category
FROM (SELECT 
Policy_Stage
,Policy_Status_Code
,Policy_Status_Category, row_number() OVER(PARTITION BY business_key ORDER BY record_eff_from_date DESC) AS rnk
    FROM  el_eds_def.dimpolicystatusmapping 
    WHERE source_app_code = 'ISIS')
WHERE  rnk = 1;


CREATE TABLE #tb_policyshield_hist AS 
SELECT  policyid, policyno, active_record_ind, policystatusid, EffectiveDate, ProductCode, record_eff_from_date, dml_ind 
from
(select policyid, policyno, active_record_ind, policystatusid, EffectiveDate, ProductCode, record_eff_from_date, dml_ind , row_number() over(partition by business_key order by coalesce(change_seq,-1) desc, record_eff_from_date desc, record_eff_to_date desc) rnk	
	from tl_ISIS_def.tb_policyshield_hist)
	where rnk=1;
CREATE TABLE #tb_policyshieldRider_hist AS 
SELECT  
record_eff_from_date 
,active_record_ind 
,dml_ind
,riderid 
,repcode 
,PolicyID   
,SubmitDate  
,IssuedDate  
,EntryDate  
,EffectiveDate  
,ExpiryDate  
,riderstatusid  
,PrevPolicyId  
,TotalPremium  
,TotalPremiumTax  
,TotalDiscountAmountTax  
,TotalLoadingAmount  
,TotalLoadingAmountTax  
,createdate   
,productcode  
,policyno  
from
(select
record_eff_from_date 
,active_record_ind 
,dml_ind
,riderid
,repcode  
,PolicyID  
,SubmitDate  
,IssuedDate  
,EntryDate  
,EffectiveDate  
,ExpiryDate  
,riderstatusid  
,PrevPolicyId  
,TotalPremium  
,TotalPremiumTax  
,TotalDiscountAmountTax  
,TotalLoadingAmount  
,TotalLoadingAmountTax  
,createdate   
,productcode  
,policyno 
,row_number() over(partition by business_key order by coalesce(change_seq,-1) desc, record_eff_from_date desc, record_eff_to_date desc) rnk	
	from tl_ISIS_def.tb_policyshieldRider_hist)
	where rnk=1;	
	
create table #tempstgDimISPolicy as(
select Policyid from(
SELECT ps.policyid, ps.policyno, ps.active_record_ind 
FROM  
#tb_policyshield_hist ps 
INNER JOIN #dimpolicystatusmapping dpsm ON ps.PolicyStatusID = dpsm.Policy_Status_Code  
WHERE dpsm.Policy_Stage = 'Issued' AND dpsm.Policy_Status_Category NOT IN ('Pending Renewal','Void') 
)
);

CREATE TABLE #dimpolicy AS 
SELECT 
    * 
FROM (SELECT 
        *, row_number() OVER(PARTITION BY business_key ORDER BY record_eff_from_date DESC) AS rnk
    FROM  el_eds_def.dimpolicy 
    WHERE source_app_code = 'ISIS')
WHERE  rnk = 1;

CREATE TABLE #PKPrimary_stg_driver_rider
AS 
SELECT policyid, riderid, record_eff_from_date,dml_ind, active_record_ind 
FROM
(select policyID,riderid, record_eff_from_date,dml_ind, active_record_ind  from #tb_policyshieldRider_hist psr 
Union
select ps.policyID,psr.riderid, ps.record_eff_from_date,ps.dml_ind, ps.active_record_ind from #tb_policyshieldRider_hist psr inner join #tb_policyshield_hist ps 
on psr.PolicyID = ps.PolicyID
);


CREATE TABLE #PKPrimary_driver_rider AS
Select policyid, riderid, active_record_ind,dml_ind
FROM(Select
policyid, 
riderid, 
active_record_ind,
dml_ind,
row_number() over( partition by policyid, riderid order by CASE WHEN dml_ind = 'D' THEN 1 else 2 END,record_eff_from_date desc ) rnk
from #PKPrimary_stg_driver_rider )
where rnk=1;




create table #tempstgFactPolicyCoveredItem as 
SELECT
b.source_app_code
,b.source_data_set
,b.dml_ind
,b.record_eff_from_date
,p.policy_covered_item_uuid
,b.business_key
,b.policy_uuid			
,b.policy_id		 
,b.policy_no		
,b.campaign_uuid
,b.product_uuid
,b.status_id as policy_status_id
,COALESCE((SELECT date_key FROM #dimdate WHERE date_key = COALESCE(CAST(TO_CHAR(p.submit_date, 'yyyyMMdd') AS NUMERIC), '-1')), -1) as submit_date_key
		,COALESCE((SELECT date_key FROM #dimdate WHERE date_key = COALESCE(CAST(TO_CHAR(p.entry_date, 'yyyyMMdd') AS NUMERIC), '-1')), -1) as entry_date_key
		,COALESCE((SELECT date_key FROM #dimdate WHERE date_key = COALESCE(CAST(TO_CHAR(p.commencement_date, 'yyyyMMdd') AS NUMERIC), '-1')), -1) as commencement_date_key
		,COALESCE((SELECT date_key FROM #dimdate WHERE date_key = COALESCE(CAST(TO_CHAR(p.issue_date, 'yyyyMMdd') AS NUMERIC), '-1')), -1) as issue_date_key
		,COALESCE((SELECT date_key FROM #dimdate WHERE date_key = COALESCE(CAST(TO_CHAR(p.start_date, 'yyyyMMdd') AS NUMERIC), '-1')), -1) as start_date_key
		,COALESCE((SELECT date_key FROM #dimdate WHERE date_key = COALESCE(CAST(TO_CHAR(p.end_date, 'yyyyMMdd') AS NUMERIC), '-1')), -1) as end_date_key
		,b.nett_premium 
        ,b.gst_on_premium
        ,b.total_premium
        ,b.annualised_premium_discount_without_gst 
        ,b.gst_on_discount_amount
        ,b.loading_amount
        ,b.gst_on_loading_amount
        ,b.spi
        ,b.annualised_premium_payable_without_gst
        ,b.wpi
        ,b.sum_assured
        ,p.sales_agent_uuid 
        ,b.servicing_agent_uuid
		,COALESCE((SELECT date_key FROM #dimdate WHERE date_key = COALESCE(CAST(TO_CHAR(p.scan_date, 'yyyyMMdd') AS NUMERIC), '-1')), -1) as scan_date_key
		,-2 as despatch_date_key
		,b.customer_uuid
		,b.policy_type_code
		,b.business_type_code
		,b.source_code
		,b.covered_item_type_code
		,b.premium_mode_id
		,b.pay_mode_id
		,b.Retained_Premium_Without_GST
		,b.standard_premium_without_gst
		,b.Annualised_Standard_Premium_Without_GST
		,b.Annualised_Occupational_Loading --Rename from [OccupationalLoading]
		,b.Other_Loading
		,b.Annualised_Disability_Loading --Rename from DisabilityLoading
		,b.Sub_Standard_Rate
		,b.Annualised_Mortality_Loading --Rename from MortalityLoading
	,b.ipmi_and_rider_discount_without_gst
	,b.ipmi_and_rider_discount_gst
	,b.ipmi_and_rider_loading_premium_without_gst
	,b.ipmi_and_rider_loading_premium_gst
	,b.mshl_additional_premium_without_gst
	,b.mshl_additional_premium_gst
	,b.mshl_premium_without_gst
	,b.mshl_premium_gst
	,b.ipmi_and_rider_premium_without_gst
	,b.ipmi_and_rider_premium_gst
	,b.mshl_premium_payable_without_gst
	,b.mshl_premium_payable_gst
	,b.ipmi_and_rider_premium_payable_without_gst
	,b.ipmi_and_rider_premium_payable_gst
	,b.mshl_pioneer_generation_subsidies_without_gst
	,b.mshl_pioneer_generation_subsidies_gst
	,b.mshl_premium_subsidies_for_lower_to_middle_income_households_without_gst
	,b.mshl_premium_subsidies_for_lower_to_middle_income_households_gst
	,b.mshl_transitional_subsidies_without_gst
	,b.mshl_transitional_subsidies_gst
	,b.mshl_premium_rebates_without_gst
	,b.mshl_premium_rebates_gst
	,b.mshl_pensioner_scheme_without_gst
	,b.mshl_pensioner_scheme_gst
	,b.Cover_Category	
	,b.ipmi_and_rider_discount_with_gst
	,b.ipmi_and_rider_loading_premium_with_gst
	,b.mshl_additional_premium_with_gst
	,b.mshl_premium_with_gst
	,b.ipmi_and_rider_premium_with_gst
	,b.mshl_premium_payable_with_gst
	,b.ipmi_and_rider_premium_payable_with_gst
	,b.mshl_pioneer_generation_subsidies_with_gst
	,b.mshl_premium_subsidies_for_lower_to_middle_income_households_with_gst
	,b.mshl_transitional_subsidies_with_gst
	,b.mshl_premium_rebates_with_gst
	,b.mshl_pensioner_scheme_with_gst
	,b.cash_benefit
	,b.accumulated_bonus
	,b.surrender_bonus
		,COALESCE((SELECT date_key FROM #dimdate WHERE date_key = COALESCE(CAST(TO_CHAR(b.non_inforce_policy_status_changed_date, 'yyyyMMdd') AS numeric), '-2')), -2) as non_inforce_policy_status_changed_date_key	
		,COALESCE((SELECT date_key FROM #dimdate WHERE date_key = COALESCE(CAST(TO_CHAR(b.reinstatement_date, 'yyyyMMdd') AS NUMERIC), '-2')), -2) as reinstatement_date_key
		,b.Annualised_Standard_Premium_With_GST
		,b.Annualised_Premium_Payable_With_GST
		,b.Annualised_Premium_Discount_With_GST
		,b.Annualised_Subsidy_Without_GST 
		,b.Annualised_Subsidy_With_GST
		,b.Annualised_Premium_Extra_Without_GST 
		,b.Annualised_Premium_Extra_With_GST
	FROM  #tempstgdimpolicycovereditem b
	INNER JOIN #dimpolicycovereditem p 
		ON b.Policy_ID = p.Policy_ID
		AND b.Covered_Item_ID = p.Covered_Item_ID
	WHERE p.source_app_code='ISIS';

CREATE TABLE #hashstgFactPolicyCoveredItem AS
	select
     source_app_code
    ,source_data_set
    ,dml_ind
    ,record_eff_from_date
    ,policy_covered_item_uuid
    ,business_key
    ,policy_uuid			
    ,policy_id		
    ,policy_no		
    ,campaign_uuid
    ,product_uuid
    ,policy_status_id
    ,submit_date_key
	,entry_date_key
	,commencement_date_key
	,issue_date_key
	,start_date_key
	,end_date_key
	,nett_premium 
    ,gst_on_premium
    ,total_premium
    ,annualised_premium_discount_without_gst 
    ,gst_on_discount_amount
    ,loading_amount
    ,gst_on_loading_amount
    ,spi
    ,annualised_premium_payable_without_gst
    ,wpi
    ,sum_assured
    ,sales_agent_uuid 
    ,servicing_agent_uuid
	,scan_date_key
	,despatch_date_key
	,customer_uuid
	,policy_type_code
	,business_type_code
	,source_code
	,covered_item_type_code
	,premium_mode_id
	,pay_mode_id
	,Retained_Premium_Without_GST
	,standard_premium_without_gst
	,Annualised_Standard_Premium_Without_GST
	,Annualised_Occupational_Loading 
	,Other_Loading
	,Annualised_Disability_Loading
	,Sub_Standard_Rate
	,Annualised_Mortality_Loading 
	,ipmi_and_rider_discount_without_gst
	,ipmi_and_rider_discount_gst
	,ipmi_and_rider_loading_premium_without_gst
	,ipmi_and_rider_loading_premium_gst
	,mshl_additional_premium_without_gst
	,mshl_additional_premium_gst
	,mshl_premium_without_gst
	,mshl_premium_gst
	,ipmi_and_rider_premium_without_gst
	,ipmi_and_rider_premium_gst
	,mshl_premium_payable_without_gst
	,mshl_premium_payable_gst
	,ipmi_and_rider_premium_payable_without_gst
	,ipmi_and_rider_premium_payable_gst
	,mshl_pioneer_generation_subsidies_without_gst
	,mshl_pioneer_generation_subsidies_gst
	,mshl_premium_subsidies_for_lower_to_middle_income_households_without_gst
	,mshl_premium_subsidies_for_lower_to_middle_income_households_gst
	,mshl_transitional_subsidies_without_gst
	,mshl_transitional_subsidies_gst
	,mshl_premium_rebates_without_gst
	,mshl_premium_rebates_gst
	,mshl_pensioner_scheme_without_gst
	,mshl_pensioner_scheme_gst
	,Cover_Category	
	,ipmi_and_rider_discount_with_gst
	,ipmi_and_rider_loading_premium_with_gst
	,mshl_additional_premium_with_gst
	,mshl_premium_with_gst
	,ipmi_and_rider_premium_with_gst
	,mshl_premium_payable_with_gst
	,ipmi_and_rider_premium_payable_with_gst
	,mshl_pioneer_generation_subsidies_with_gst
	,mshl_premium_subsidies_for_lower_to_middle_income_households_with_gst
	,mshl_transitional_subsidies_with_gst
	,mshl_premium_rebates_with_gst
	,mshl_pensioner_scheme_with_gst
	,cash_benefit
	,accumulated_bonus
	,surrender_bonus
	,non_inforce_policy_status_changed_date_key	
	,reinstatement_date_key
	,Annualised_Standard_Premium_With_GST
	,Annualised_Premium_Payable_With_GST
	,Annualised_Premium_Discount_With_GST
	,Annualised_Subsidy_Without_GST
	,Annualised_Subsidy_With_GST
	,Annualised_Premium_Extra_Without_GST 
	,Annualised_Premium_Extra_With_GST
	,sha2(
coalesce(cast(source_app_code as varchar),cast('null' as varchar))+
coalesce(cast(source_data_set as varchar),cast('null' as varchar))+
coalesce(cast(policy_covered_item_uuid as varchar),cast('null' as varchar))+
coalesce(cast(business_key as varchar),cast('null' as varchar))+
coalesce(cast(policy_uuid as varchar),cast('null' as varchar))+
coalesce(cast(policy_id as varchar),cast('null' as varchar))+
coalesce(cast(policy_no as varchar),cast('null' as varchar))+
coalesce(cast(campaign_uuid as varchar),cast('null' as varchar))+
coalesce(cast(product_uuid as varchar),cast('null' as varchar))+
coalesce(cast(policy_status_id as varchar),cast('null' as varchar))+
coalesce(cast(submit_date_key as varchar),cast('null' as varchar))+
coalesce(cast(entry_date_key as varchar),cast('null' as varchar))+
coalesce(cast(commencement_date_key as varchar),cast('null' as varchar))+
coalesce(cast(issue_date_key as varchar),cast('null' as varchar))+
coalesce(cast(start_date_key as varchar),cast('null' as varchar))+
coalesce(cast(end_date_key as varchar),cast('null' as varchar))+
coalesce(cast(nett_premium  as varchar),cast('null' as varchar))+
coalesce(cast(gst_on_premium as varchar),cast('null' as varchar))+
coalesce(cast(total_premium as varchar),cast('null' as varchar))+
coalesce(cast(annualised_premium_discount_without_gst  as varchar),cast('null' as varchar))+
coalesce(cast(gst_on_discount_amount as varchar),cast('null' as varchar))+
coalesce(cast(loading_amount as varchar),cast('null' as varchar))+
coalesce(cast(gst_on_loading_amount as varchar),cast('null' as varchar))+
coalesce(cast(spi as varchar),cast('null' as varchar))+
coalesce(cast(annualised_premium_payable_without_gst as varchar),cast('null' as varchar))+
coalesce(cast(wpi as varchar),cast('null' as varchar))+
coalesce(cast(sum_assured as varchar),cast('null' as varchar))+
coalesce(cast(sales_agent_uuid  as varchar),cast('null' as varchar))+
coalesce(cast(servicing_agent_uuid as varchar),cast('null' as varchar))+
coalesce(cast(scan_date_key as varchar),cast('null' as varchar))+
coalesce(cast(despatch_date_key as varchar),cast('null' as varchar))+
coalesce(cast(customer_uuid as varchar),cast('null' as varchar))+
coalesce(cast(policy_type_code as varchar),cast('null' as varchar))+
coalesce(cast(business_type_code as varchar),cast('null' as varchar))+
coalesce(cast(source_code as varchar),cast('null' as varchar))+
coalesce(cast(covered_item_type_code as varchar),cast('null' as varchar))+
coalesce(cast(premium_mode_id as varchar),cast('null' as varchar))+
coalesce(cast(pay_mode_id as varchar),cast('null' as varchar))+
coalesce(cast(Retained_Premium_Without_GST as varchar),cast('null' as varchar))+
coalesce(cast(standard_premium_without_gst as varchar),cast('null' as varchar))+
coalesce(cast(Annualised_Standard_Premium_Without_GST as varchar),cast('null' as varchar))+
coalesce(cast(Annualised_Occupational_Loading  as varchar),cast('null' as varchar))+
coalesce(cast(Other_Loading as varchar),cast('null' as varchar))+
coalesce(cast(Annualised_Disability_Loading as varchar),cast('null' as varchar))+
coalesce(cast(Sub_Standard_Rate as varchar),cast('null' as varchar))+
coalesce(cast(Annualised_Mortality_Loading  as varchar),cast('null' as varchar))+
coalesce(cast(ipmi_and_rider_discount_without_gst as varchar),cast('null' as varchar))+
coalesce(cast(ipmi_and_rider_discount_gst as varchar),cast('null' as varchar))+
coalesce(cast(ipmi_and_rider_loading_premium_without_gst as varchar),cast('null' as varchar))+
coalesce(cast(ipmi_and_rider_loading_premium_gst as varchar),cast('null' as varchar))+
coalesce(cast(mshl_additional_premium_without_gst as varchar),cast('null' as varchar))+
coalesce(cast(mshl_additional_premium_gst as varchar),cast('null' as varchar))+
coalesce(cast(mshl_premium_without_gst as varchar),cast('null' as varchar))+
coalesce(cast(mshl_premium_gst as varchar),cast('null' as varchar))+
coalesce(cast(ipmi_and_rider_premium_without_gst as varchar),cast('null' as varchar))+
coalesce(cast(ipmi_and_rider_premium_gst as varchar),cast('null' as varchar))+
coalesce(cast(mshl_premium_payable_without_gst as varchar),cast('null' as varchar))+
coalesce(cast(mshl_premium_payable_gst as varchar),cast('null' as varchar))+
coalesce(cast(ipmi_and_rider_premium_payable_without_gst as varchar),cast('null' as varchar))+
coalesce(cast(ipmi_and_rider_premium_payable_gst as varchar),cast('null' as varchar))+
coalesce(cast(mshl_pioneer_generation_subsidies_without_gst as varchar),cast('null' as varchar))+
coalesce(cast(mshl_pioneer_generation_subsidies_gst as varchar),cast('null' as varchar))+
coalesce(cast(mshl_premium_subsidies_for_lower_to_middle_income_households_without_gst as varchar),cast('null' as varchar))+
coalesce(cast(mshl_premium_subsidies_for_lower_to_middle_income_households_gst as varchar),cast('null' as varchar))+
coalesce(cast(mshl_transitional_subsidies_without_gst as varchar),cast('null' as varchar))+
coalesce(cast(mshl_transitional_subsidies_gst as varchar),cast('null' as varchar))+
coalesce(cast(mshl_premium_rebates_without_gst as varchar),cast('null' as varchar))+
coalesce(cast(mshl_premium_rebates_gst as varchar),cast('null' as varchar))+
coalesce(cast(mshl_pensioner_scheme_without_gst as varchar),cast('null' as varchar))+
coalesce(cast(mshl_pensioner_scheme_gst as varchar),cast('null' as varchar))+
coalesce(cast(Cover_Category as varchar),cast('null' as varchar))+
coalesce(cast(ipmi_and_rider_discount_with_gst as varchar),cast('null' as varchar))+
coalesce(cast(ipmi_and_rider_loading_premium_with_gst as varchar),cast('null' as varchar))+
coalesce(cast(mshl_additional_premium_with_gst as varchar),cast('null' as varchar))+
coalesce(cast(mshl_premium_with_gst as varchar),cast('null' as varchar))+
coalesce(cast(ipmi_and_rider_premium_with_gst as varchar),cast('null' as varchar))+
coalesce(cast(mshl_premium_payable_with_gst as varchar),cast('null' as varchar))+
coalesce(cast(ipmi_and_rider_premium_payable_with_gst as varchar),cast('null' as varchar))+
coalesce(cast(mshl_pioneer_generation_subsidies_with_gst as varchar),cast('null' as varchar))+
coalesce(cast(mshl_premium_subsidies_for_lower_to_middle_income_households_with_gst as varchar),cast('null' as varchar))+
coalesce(cast(mshl_transitional_subsidies_with_gst as varchar),cast('null' as varchar))+
coalesce(cast(mshl_premium_rebates_with_gst as varchar),cast('null' as varchar))+
coalesce(cast(mshl_pensioner_scheme_with_gst as varchar),cast('null' as varchar))+
coalesce(cast(cash_benefit as varchar),cast('null' as varchar))+
coalesce(cast(accumulated_bonus as varchar),cast('null' as varchar))+
coalesce(cast(surrender_bonus as varchar),cast('null' as varchar))+
coalesce(cast(non_inforce_policy_status_changed_date_key as varchar),cast('null' as varchar))+
coalesce(cast(reinstatement_date_key as varchar),cast('null' as varchar))+
coalesce(cast(Annualised_Standard_Premium_With_GST as varchar),cast('null' as varchar))+
coalesce(cast(Annualised_Premium_Payable_With_GST as varchar),cast('null' as varchar))+
coalesce(cast(Annualised_Premium_Discount_With_GST as varchar),cast('null' as varchar))+
coalesce(cast(Annualised_Subsidy_Without_GST as varchar),cast('null' as varchar))+
coalesce(cast(Annualised_Subsidy_With_GST as varchar),cast('null' as varchar))+
coalesce(cast(Annualised_Premium_Extra_Without_GST  as varchar),cast('null' as varchar))+
coalesce(cast(Annualised_Premium_Extra_With_GST as varchar),cast('null' as varchar)), 256) as checksum from #tempstgFactPolicyCoveredItem;

CREATE TABLE #stgFactPolicyCoveredItem AS
select * from (select a.* , case when a.checksum <> coalesce(b.checksum,'1') then 1 when a.dml_ind='D' then 1 else 0 end as changed_rec_check from #hashstgFactPolicyCoveredItem a LEFT OUTER JOIN el_eds_def.factpolicycovereditem b on a.business_key = b.business_key AND b.source_app_code='ISIS'  and b.active_record_ind='Y' ) where changed_rec_check =1;

DELETE FROM el_eds_def_stg.stgfactpolicycovereditem where source_app_code='ISIS';

INSERT INTO el_eds_def_stg.stgfactpolicycovereditem
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
    ,policy_covered_item_uuid
    ,business_key
    ,policy_uuid
    ,policy_id
    ,policy_no
    ,campaign_uuid
    ,product_uuid
    ,policy_status_id
    ,submit_date_key
    ,entry_date_key
    ,commencement_date_key
    ,issue_date_key
    ,start_date_key
    ,end_date_key
    ,nett_premium 
    ,gst_on_premium
    ,total_premium
    ,annualised_premium_discount_without_gst 
    ,gst_on_discount_amount
    ,loading_amount
    ,gst_on_loading_amount
    ,spi
    ,annualised_premium_payable_without_gst
    ,wpi
    ,sum_assured
    ,sales_agent_uuid 
    ,servicing_agent_uuid
    ,scan_date_key
    ,despatch_date_key
    ,customer_uuid
    ,policy_type_code
    ,business_type_code
    ,source_code
    ,covered_item_type_code
    ,premium_mode_id
    ,pay_mode_id
    ,Retained_Premium_Without_GST
    ,standard_premium_without_gst
    ,Annualised_Standard_Premium_Without_GST
    ,Annualised_Occupational_Loading 
    ,Other_Loading
    ,Annualised_Disability_Loading
    ,Sub_Standard_Rate
    ,Annualised_Mortality_Loading 
	       ,Commission
           ,Coinsurance_Premium
           ,Coinsurance_Commission
           ,Annual_Gross_Premium
           ,Standard_Gross_Premium
           ,Standard_Nett_Premium
           ,Adjusted_Net_Premium
           ,Premium_Rebate
           ,SI_Currency_Type
           ,Gross_Premium
           ,Gross_Commission
           ,Loan_Rate
           ,Annualised_Policy_Fee --Rename from [PolicyFee]
           ,Sum_Assured_Bonus_Cover
    ,ipmi_and_rider_discount_without_gst
    ,ipmi_and_rider_discount_gst
    ,ipmi_and_rider_loading_premium_without_gst
    ,ipmi_and_rider_loading_premium_gst
    ,mshl_additional_premium_without_gst
    ,mshl_additional_premium_gst
    ,mshl_premium_without_gst
    ,mshl_premium_gst
    ,ipmi_and_rider_premium_without_gst
    ,ipmi_and_rider_premium_gst
    ,mshl_premium_payable_without_gst
    ,mshl_premium_payable_gst
    ,ipmi_and_rider_premium_payable_without_gst
    ,ipmi_and_rider_premium_payable_gst
    ,mshl_pioneer_generation_subsidies_without_gst
    ,mshl_pioneer_generation_subsidies_gst
    ,mshl_premium_subsidies_for_lower_to_middle_income_households_without_gst
    ,mshl_premium_subsidies_for_lower_to_middle_income_households_gst
    ,mshl_transitional_subsidies_without_gst
    ,mshl_transitional_subsidies_gst
    ,mshl_premium_rebates_without_gst
    ,mshl_premium_rebates_gst
    ,mshl_pensioner_scheme_without_gst
    ,mshl_pensioner_scheme_gst
    ,Cover_Category
	,Premium_Status_Id
	,corp_sales_agent_uuid
	,Bonus_Cover_Date_Key
    ,ipmi_and_rider_discount_with_gst
    ,ipmi_and_rider_loading_premium_with_gst
    ,mshl_additional_premium_with_gst
    ,mshl_premium_with_gst
    ,ipmi_and_rider_premium_with_gst
    ,mshl_premium_payable_with_gst
    ,ipmi_and_rider_premium_payable_with_gst
    ,mshl_pioneer_generation_subsidies_with_gst
    ,mshl_premium_subsidies_for_lower_to_middle_income_households_with_gst
    ,mshl_transitional_subsidies_with_gst
    ,mshl_premium_rebates_with_gst
    ,mshl_pensioner_scheme_with_gst
    ,cash_benefit
    ,accumulated_bonus
    ,surrender_bonus
    ,non_inforce_policy_status_changed_date_key
    ,reinstatement_date_key
    ,Annualised_Standard_Premium_With_GST
    ,Annualised_Premium_Payable_With_GST
    ,Annualised_Premium_Discount_With_GST
    ,Annualised_Subsidy_Without_GST
    ,Annualised_Subsidy_With_GST
    ,Annualised_Premium_Extra_Without_GST 
    ,Annualised_Premium_Extra_With_GST
    ,Conversion_Amount
    ,checksum	
	)
select
      x.source_app_code
	  ,x.source_data_set
	  ,x.dml_ind
	  ,getdate() as record_created_date
	  ,getdate() as record_updated_date
	  ,'EDS' as record_created_by
	  ,'EDS' as record_updated_by
	  ,x.record_eff_from_date
	  ,cast('9999-12-31 00:00:00.000000' as timestamp) as record_eff_to_date
	  ,'Y' as active_record_ind
    ,x.policy_covered_item_uuid
    ,x.business_key
    ,x.policy_uuid
    ,x.policy_id
    ,x.policy_no
    ,x.campaign_uuid
    ,x.product_uuid
    ,x.policy_status_id
    ,x.submit_date_key
    ,x.entry_date_key
    ,x.commencement_date_key
    ,x.issue_date_key
    ,x.start_date_key
    ,x.end_date_key
    ,x.nett_premium 
    ,x.gst_on_premium
    ,x.total_premium
    ,x.annualised_premium_discount_without_gst 
    ,x.gst_on_discount_amount
    ,x.loading_amount
    ,x.gst_on_loading_amount
    ,x.spi
    ,x.annualised_premium_payable_without_gst
    ,x.wpi
    ,x.sum_assured
    ,x.sales_agent_uuid 
    ,x.servicing_agent_uuid
    ,x.scan_date_key
    ,x.despatch_date_key
    ,x.customer_uuid
    ,x.policy_type_code
    ,x.business_type_code
    ,x.source_code
    ,x.covered_item_type_code
    ,x.premium_mode_id
    ,x.pay_mode_id
    ,x.Retained_Premium_Without_GST
    ,x.standard_premium_without_gst
    ,x.Annualised_Standard_Premium_Without_GST
    ,x.Annualised_Occupational_Loading 
    ,x.Other_Loading
    ,x.Annualised_Disability_Loading
    ,x.Sub_Standard_Rate
    ,x.Annualised_Mortality_Loading 
	       ,0 as Commission
           ,0 as Coinsurance_Premium
           ,0 as Coinsurance_Commission
           ,0 as Annual_Gross_Premium
           ,0 as Standard_Gross_Premium
           ,0 as Standard_Nett_Premium
           ,0 as Adjusted_Net_Premium
           ,0 as Premium_Rebate
           ,NULL as SI_Currency_Type
           ,0 as Gross_Premium
           ,0 as Gross_Commission
           ,0 as Loan_Rate
           ,0 as Annualised_Policy_Fee --Rename from [PolicyFee]
           ,0 as Sum_Assured_Bonus_Cover
    ,x.ipmi_and_rider_discount_without_gst
    ,x.ipmi_and_rider_discount_gst
    ,x.ipmi_and_rider_loading_premium_without_gst
    ,x.ipmi_and_rider_loading_premium_gst
    ,x.mshl_additional_premium_without_gst
    ,x.mshl_additional_premium_gst
    ,x.mshl_premium_without_gst
    ,x.mshl_premium_gst
    ,x.ipmi_and_rider_premium_without_gst
    ,x.ipmi_and_rider_premium_gst
    ,x.mshl_premium_payable_without_gst
    ,x.mshl_premium_payable_gst
    ,x.ipmi_and_rider_premium_payable_without_gst
    ,x.ipmi_and_rider_premium_payable_gst
    ,x.mshl_pioneer_generation_subsidies_without_gst
    ,x.mshl_pioneer_generation_subsidies_gst
    ,x.mshl_premium_subsidies_for_lower_to_middle_income_households_without_gst
    ,x.mshl_premium_subsidies_for_lower_to_middle_income_households_gst
    ,x.mshl_transitional_subsidies_without_gst
    ,x.mshl_transitional_subsidies_gst
    ,x.mshl_premium_rebates_without_gst
    ,x.mshl_premium_rebates_gst
    ,x.mshl_pensioner_scheme_without_gst
    ,x.mshl_pensioner_scheme_gst
    ,x.Cover_Category
	,-1 as Premium_Status_Id
	,-1 as corp_sales_agent_uuid
	,-1 as Bonus_Cover_Date_Key
    ,x.ipmi_and_rider_discount_with_gst
    ,x.ipmi_and_rider_loading_premium_with_gst
    ,x.mshl_additional_premium_with_gst
    ,x.mshl_premium_with_gst
    ,x.ipmi_and_rider_premium_with_gst
    ,x.mshl_premium_payable_with_gst
    ,x.ipmi_and_rider_premium_payable_with_gst
    ,x.mshl_pioneer_generation_subsidies_with_gst
    ,x.mshl_premium_subsidies_for_lower_to_middle_income_households_with_gst
    ,x.mshl_transitional_subsidies_with_gst
    ,x.mshl_premium_rebates_with_gst
    ,x.mshl_pensioner_scheme_with_gst
    ,x.cash_benefit
    ,x.accumulated_bonus
    ,x.surrender_bonus
    ,x.non_inforce_policy_status_changed_date_key
    ,x.reinstatement_date_key
    ,x.Annualised_Standard_Premium_With_GST
    ,x.Annualised_Premium_Payable_With_GST
    ,x.Annualised_Premium_Discount_With_GST
    ,x.Annualised_Subsidy_Without_GST
    ,x.Annualised_Subsidy_With_GST
    ,x.Annualised_Premium_Extra_Without_GST 
    ,x.Annualised_Premium_Extra_With_GST
    ,0 as Conversion_Amount
    ,x.checksum
from (
    select
	 source_app_code
    ,source_data_set
    ,dml_ind
    ,record_eff_from_date
    ,policy_covered_item_uuid
    ,business_key
    ,policy_uuid
    ,policy_id
    ,policy_no
    ,campaign_uuid
    ,product_uuid
    ,policy_status_id
    ,submit_date_key
    ,entry_date_key
    ,commencement_date_key
    ,issue_date_key
    ,start_date_key
    ,end_date_key
    ,nett_premium 
    ,gst_on_premium
    ,total_premium
    ,annualised_premium_discount_without_gst 
    ,gst_on_discount_amount
    ,loading_amount
    ,gst_on_loading_amount
    ,spi
    ,annualised_premium_payable_without_gst
    ,wpi
    ,sum_assured
    ,sales_agent_uuid 
    ,servicing_agent_uuid
    ,scan_date_key
    ,despatch_date_key
    ,customer_uuid
    ,policy_type_code
    ,business_type_code
    ,source_code
    ,covered_item_type_code
    ,premium_mode_id
    ,pay_mode_id
    ,Retained_Premium_Without_GST
    ,standard_premium_without_gst
    ,Annualised_Standard_Premium_Without_GST
    ,Annualised_Occupational_Loading 
    ,Other_Loading
    ,Annualised_Disability_Loading
    ,Sub_Standard_Rate
    ,Annualised_Mortality_Loading 
	,ipmi_and_rider_discount_without_gst
    ,ipmi_and_rider_discount_gst
    ,ipmi_and_rider_loading_premium_without_gst
    ,ipmi_and_rider_loading_premium_gst
    ,mshl_additional_premium_without_gst
    ,mshl_additional_premium_gst
    ,mshl_premium_without_gst
    ,mshl_premium_gst
    ,ipmi_and_rider_premium_without_gst
    ,ipmi_and_rider_premium_gst
    ,mshl_premium_payable_without_gst
    ,mshl_premium_payable_gst
    ,ipmi_and_rider_premium_payable_without_gst
    ,ipmi_and_rider_premium_payable_gst
    ,mshl_pioneer_generation_subsidies_without_gst
    ,mshl_pioneer_generation_subsidies_gst
    ,mshl_premium_subsidies_for_lower_to_middle_income_households_without_gst
    ,mshl_premium_subsidies_for_lower_to_middle_income_households_gst
    ,mshl_transitional_subsidies_without_gst
    ,mshl_transitional_subsidies_gst
    ,mshl_premium_rebates_without_gst
    ,mshl_premium_rebates_gst
    ,mshl_pensioner_scheme_without_gst
    ,mshl_pensioner_scheme_gst
    ,Cover_Category
	,ipmi_and_rider_discount_with_gst
    ,ipmi_and_rider_loading_premium_with_gst
    ,mshl_additional_premium_with_gst
    ,mshl_premium_with_gst
    ,ipmi_and_rider_premium_with_gst
    ,mshl_premium_payable_with_gst
    ,ipmi_and_rider_premium_payable_with_gst
    ,mshl_pioneer_generation_subsidies_with_gst
    ,mshl_premium_subsidies_for_lower_to_middle_income_households_with_gst
    ,mshl_transitional_subsidies_with_gst
    ,mshl_premium_rebates_with_gst
    ,mshl_pensioner_scheme_with_gst
    ,cash_benefit
    ,accumulated_bonus
    ,surrender_bonus
    ,non_inforce_policy_status_changed_date_key
    ,reinstatement_date_key
    ,Annualised_Standard_Premium_With_GST
    ,Annualised_Premium_Payable_With_GST
    ,Annualised_Premium_Discount_With_GST
    ,Annualised_Subsidy_Without_GST
    ,Annualised_Subsidy_With_GST
    ,Annualised_Premium_Extra_Without_GST 
    ,Annualised_Premium_Extra_With_GST
	,checksum
	FROM #stgFactPolicyCoveredItem) x;
	

----------------audit---------------
create table #min_record_eff_from_date as
select
  min(record_eff_from_date) AS min_record_eff_from_date,
  'ISIS-MAIN'::VARCHAR(50) AS source_data_set, 
  'ISIS'::VARCHAR(50) AS source_app_code
from
  (
    select max(record_eff_from_date) as record_eff_from_date from #tb_policyshield_hist where active_record_ind = 'Y'
	Union   
	select max(record_eff_from_date) as record_eff_from_date from tl_isis_def.tb_ispremiummain_hist where active_record_ind = 'Y'
);
    


insert into #min_record_eff_from_date 
select
  min(record_eff_from_date) as min_record_eff_from_date,
  'ISIS-RIDER' AS source_data_set, 
  'ISIS' AS source_app_code
from
  (
    select max(record_eff_from_date) as record_eff_from_date from #tb_policyshield_hist where active_record_ind = 'Y'
	Union  
	select max(record_eff_from_date) as record_eff_from_date from #tb_policyshieldrider_hist where active_record_ind = 'Y'
	Union
	select max(record_eff_from_date) as record_eff_from_date from tl_isis_def.tb_ispremiumrider_hist where active_record_ind = 'Y'
	Union
	select max(record_eff_from_date) as record_eff_from_date from tl_ISIS_def.tb_policytermination_hist where active_record_ind = 'Y'
    
  );

DELETE FROM el_eds_def_stg.ctrl_audit_stg where tgt_table_name= 'factpolicycovereditem' and tgt_source_app_code='ISIS';

Insert Into el_eds_def_stg.ctrl_audit_stg
select
  source_data_set as tgt_source_data_set,
  source_app_code tgt_source_app_code,
  'el_eds_def' as tgt_schema,
  'factpolicycovereditem' as tgt_table_name,
  min_record_eff_from_date as src_record_eff_from_date,
  Null as tgt_record_eff_from_date,
  getdate() as data_pipeline_run_date,
  getdate() as record_created_date,
  getdate() as record_updated_date
from #min_record_eff_from_date;
--------------------------------------------------
--------------------------------------------------

create table #src_count as(
select
  count(1) as source_count, 'tl_isis_def' as src_schema, 'tb_policyshield_hist'::VARCHAR(1000) as src_table, 'ISIS-MAIN'::VARCHAR(50) as entity
from
  (
    SELECT DISTINCT ps.policyid, ps.policyno, ps.active_record_ind 
	FROM #tempstgDimISPolicy a INNER JOIN #tb_policyshield_hist ps ON a.PolicyID = ps.PolicyID
	INNER JOIN #DimPolicy dp ON a.PolicyID = dp.Policy_ID
	)
where
  ACTIVE_RECORD_IND='Y'
);


insert Into #src_count (
select
  count(1) as source_count, 'tl_isis_def' as src_schema, 'tb_policyshieldrider_hist' as src_table, 'ISIS-RIDER' as entity
from
  (
   SELECT psr.policyid, psr.riderid, pk.active_record_ind 
	from #PKPrimary_driver_rider pk inner join #tb_PolicyshieldRider_hist psr 
	on pk.Policyid=psr.Policyid  and pk.riderid=psr.riderid
	INNER JOIN #tb_policyshield_hist ps ON psr.PolicyID = ps.PolicyID
	INNER JOIN #dimpolicy dp ON  psr.policyid = dp.Policy_Id
	INNER JOIN #dimpolicystatusmapping dpsm ON psr.riderstatusid = dpsm.Policy_Status_Code 
	WHERE dpsm.Policy_Stage = 'Issued' AND dpsm.Policy_Status_Category NOT IN ('Pending Renewal','Void')  
	)
where 
	ACTIVE_RECORD_IND='Y'
);
  


DELETE FROM el_eds_def_stg.dq_audit_stg WHERE entity in (select entity from #src_count) and tgt_table ='factpolicycovereditem';

insert into
  el_eds_def_stg.dq_audit_stg(
    select
      'eds' as app_name,
      src_schema,
      src_table,
      'el_eds_def' as tgt_schema,
      'factpolicycovereditem' as tgt_table,
      entity,
      '*' as instance,
      'count' as check_type,
      source_count
	from #src_count);


DROP TABLE IF EXISTS #dimdate;
DROP TABLE IF EXISTS #dimpolicycovereditem;
DROP TABLE IF EXISTS #tempstgdimpolicycovereditem;
DROP TABLE IF EXISTS #hashstgFactPolicyCoveredItem;
DROP TABLE IF EXISTS #stgFactPolicyCoveredItem;
DROP TABLE IF EXISTS #tempstgFactPolicyCoveredItem;
	
END;