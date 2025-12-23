/*
Amendment History:
CRQ                     Modified date           modified by       	  description
CRQ000000238343          16-07-2025                Vignesh            UPPER function changes related to Dimdiagnosis.diagnosis_code 
CJira-CRDTLK-73	      	 19-11-2025     	       Vignesh        	  UPPER function changes related to tl_wbcs_def.tb_DiagnosisCode_hist.diagnosis_code
*/
BEGIN;

SET TIMEZONE = 'Singapore';

INSERT INTO el_eds_def.dimclaim(
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
					business_key
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
					,policy_no)
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
					-1 AS claim_uuid,
					('MANUAL' || '~' || -1) AS business_key
					,NULL as claim_event_id
					,NULL as claim_id
					,NULL as claim_event_no
					,NULL as claim_no
					,NULL as claim_event_occurance_date
					,NULL as claim_event_location
					,NULL as claim_event_reporting_24hrs
					,-1 as claim_event_type_id
					,-1 as claim_type_id
					,-1 as claim_event_status_id
					,-1 as claim_status_id
					,NULL as claim_event_closed_date
					,NULL as claim_closed_date
					,NULL as country_of_accident
					,NULL as notification_date
					,NULL as claim_reported_date
					,-1 as claim_level_id
					,-1 as claim_source_id
					,-1 as claim_damage_type_id
					,-1 as claim_injury_type_id
					,NULL as towing_required
					,-1 as tp_clinic_uuid
					,NULL as tp_driver_nric
					,-1 as tp_insurer_uuid
					,-1 as tp_lawyer_uuid
					,-1 as tp_surveyor_uuid
					,NULL as tp_vehicle_no
					,-1 as tp_workshop_uuid
					,NULL as workshop_repairer
					,NULL as no_adl
					,NULL as claim_insured_liability
					,NULL as claim_insured_liability_desc
					,NULL as claim_desc
					,NULL as name_of_preferred_workshop
					,NULL as orange_force
					,NULL as our_lawyer
					,NULL as claim_submission_type
					,NULL as icm_no
					,NULL as tca
					,NULL as wbcs_flag
					,NULL as ecode
					,NULL as ecode_reason
					,NULL as od_excess
					,NULL as tp_excess
					,NULL as additional_excess
					,NULL as windscreen_excess
					,NULL as unnamed_driver_excess
					,'-1'as claim_officer_staff_uuid
					,'-1' as claim_creator_staff_uuid
					,NULL as wbcs_claim_no
					,NULL as claim_status_loss
					,NULL as claim_status_recovery
					,NULL as claim_status_salvage
					,NULL as cause_of_loss
					,'-1' as policy_uuid
					,'-1' as customer_uuid
					,'-1'as product_uuid
					,'-1' as sub_product_uuid
					,'-1'as sales_agent_uuid
					,NULL as sales_agent_code
					,'-1'as servicing_agent_uuid
					,NULL as claim_occurrance_time_hhmm
					,-1 as claimant_type_id
					,NULL as gst_registered
					,NULL as gst_verified
					,NULL as claim_event_type_code
					,NULL as claim_type_code
					,NULL as claim_event_status_code
					,NULL as claim_status_code
					,NULL as policy_id
					,NULL as policy_no
					WHERE (
						SELECT COUNT(1) FROM el_eds_def.dimclaim WHERE claim_uuid = '-1'
						) = 0;

CREATE TABLE #v_rundate AS
SELECT nvl(CAST(DATE_TRUNC('day',DATEADD (day,-1,src_record_eff_from_date)) AS TIMESTAMP),CAST('1900-01-01 00:00:00.000' AS TIMESTAMP)) AS v_vLastRunDate
FROM (SELECT MAX(src_record_eff_from_date) AS src_record_eff_from_date FROM el_eds_def_stg.ctrl_audit 
WHERE tgt_table_name='dimclaim' and tgt_source_app_code='WBCS');
 
CREATE TABLE #tb_claims_hist AS 
SELECT  
     dml_ind
	 ,active_record_ind
    ,record_eff_from_date
	,record_created_date
    ,ClaimNo
	,DateOcc
	,Status
	,DateClosed
	,DateClaim
	,ClaimDesc
	,ClaimType
	,PolicyNo
	,AmtClaimed
	,AmtPaid
	,Reserve
	,PolicyID
from
(		
select 
	 dml_ind
	 ,active_record_ind
    ,record_eff_from_date
	,record_created_date
    ,cast(ClaimNo as varchar(100))
	,DateOcc
	,Status
	,DateClosed
	,DateClaim
	,ClaimDesc
	,ClaimType
	,PolicyNo
	,AmtClaimed
	,AmtPaid
	,Reserve
	,PolicyID
	,row_number() over(partition by business_key order by coalesce(change_seq,-1) desc, record_eff_from_date desc, record_eff_to_date desc) rnk	
	from tl_wbcs_def.tb_claims_hist)
	where rnk=1 ;

CREATE TABLE #tb_isclaims_hist AS 
SELECT  
     dml_ind
    ,record_eff_from_date
    ,BillCategory
	,HospCode
	,HOSTYPE
	,RM_BDATE_FROM
	,RM_BDATE_TO
	,SPECIALIST
	,SpecialityDesc
	,MEDICODE
	,OtherDiagnosis
	,ProRationPercent
	,AMTPAID_TP
	,COINSURANCE
	,HRN
	,Total_IS_Pays
	,HCBValue
	,EBBClaimAmt
	,RiderWaiver
	,PayablePolicyRider
	,RiderPrdtCode
	,DEDUCTIBLES
	,AssR_CoInsure
	,BAL_POLICY_YR
	,BAL_LIFETIME
	,SB1OpCode
	,SB2OpCode
	,SB3OpCode
	,SB4OpCode
	,SB5OpCode
	,SB6OpCode
	,SB7OpCode
	,SB8OpCode
	,SB9OpCode
	,SB10OpCode
	,RM2_BDATE_FROM
	,RM2_BDATE_TO
	,RM3_BDATE_FROM
	,RM3_BDATE_TO
	,Sub_Type
	,cast(CLAIMNO as varchar(100))
	,Ward
	,warddesc
	,hospcodedesc
    ,active_record_ind
    from(select
	dml_ind
    ,record_eff_from_date
    ,BillCategory
	,HospCode
	,HOSTYPE
	,RM_BDATE_FROM
	,RM_BDATE_TO
	,SPECIALIST
	,SpecialityDesc
	,MEDICODE
	,OtherDiagnosis
	,ProRationPercent
	,AMTPAID_TP
	,COINSURANCE
	,HRN
	,Total_IS_Pays
	,HCBValue
	,EBBClaimAmt
	,RiderWaiver
	,PayablePolicyRider
	,RiderPrdtCode
	,DEDUCTIBLES
	,AssR_CoInsure
	,BAL_POLICY_YR
	,BAL_LIFETIME
	,SB1OpCode
	,SB2OpCode
	,SB3OpCode
	,SB4OpCode
	,SB5OpCode
	,SB6OpCode
	,SB7OpCode
	,SB8OpCode
	,SB9OpCode
	,SB10OpCode
	,RM2_BDATE_FROM
	,RM2_BDATE_TO
	,RM3_BDATE_FROM
	,RM3_BDATE_TO
	,Sub_Type
	,CLAIMNO
	,Ward
	,warddesc
	,hospcodedesc
    ,active_record_ind
	,row_number() over(partition by business_key order by coalesce(change_seq,-1) desc, record_eff_from_date desc, record_eff_to_date desc) rnk
	from tl_wbcs_def.tb_isclaims_hist)
	where rnk=1;

CREATE TABLE #tempstgDimISClaim AS
SELECT *
FROM el_eds_def_stg.tempstgDimISClaim 
where SOURCE_APP_CODE = 'WBCS';


CREATE TABLE #DimClaimEventTypeMapping AS
SELECT *
FROM el_eds_def.DimClaimEventTypeMapping 
where (ACTIVE_RECORD_IND = 'Y')
AND SOURCE_APP_CODE = 'WBCS';	

CREATE TABLE #DimClaimTypeMapping AS
SELECT *
FROM el_eds_def.DimClaimTypeMapping 
where (ACTIVE_RECORD_IND = 'Y')
AND SOURCE_APP_CODE = 'WBCS';	

CREATE TABLE #DimClaimEventStatusMapping AS
SELECT *
FROM el_eds_def.DimClaimEventStatusMapping 
where (ACTIVE_RECORD_IND = 'Y')
AND SOURCE_APP_CODE = 'WBCS';

CREATE TABLE #DimClaimStatusMapping AS
SELECT *
FROM el_eds_def.DimClaimStatusMapping 
where (ACTIVE_RECORD_IND = 'Y')
AND SOURCE_APP_CODE = 'WBCS';

CREATE TABLE #DimDiagnosis AS
SELECT diagnosis_uuid
,Diagnosis_Code
,active_record_ind
FROM el_eds_def.DimDiagnosis 
where (ACTIVE_RECORD_IND = 'Y');

CREATE TABLE #DimClaim AS
SELECT Policy_ID,
Claim_No,
Policy_No
FROM el_eds_def.DimClaim 
where (ACTIVE_RECORD_IND = 'Y')
AND SOURCE_APP_CODE = 'WBCS';

CREATE TABLE #DimPolicy AS
SELECT Policy_uuid
,Customer_uuid
,Product_uuid
,Sub_Product_uuid
,Sales_Agent_uuid
,Servicing_Agent_uuid
,Policy_ID
,Policy_No
,Policy_Start_Date
,Policy_End_Date
FROM el_eds_def.DimPolicy
where (ACTIVE_RECORD_IND = 'Y')
AND SOURCE_APP_CODE = 'ISIS';


CREATE TABLE #DimAgent AS
SELECT Agent_Code,
Agent_uuid
FROM el_eds_def.DimAgent 
where (ACTIVE_RECORD_IND = 'Y');

CREATE TABLE #tb_isclaimsdetails_hist AS 
SELECT  
     dml_ind
    ,record_eff_from_date
	,TOTAL_EXP
	,EXPMISC
	,MISC_DESC
	,EXPSB
	,EXPSB2
	,EXPSB3
	,EXPSB4
	,EXPSB5
	,EXPSB6
	,EXPSB7
	,EXPSB8
	,EXPSB9
	,EXPSB10
	,CLAIMAMTRB
	,EXPRB
	,LIM_RB1
	,NUMDAYRB
	,CLAIMAMTRB2
	,EXPRB2
	,LIM_RB2
	,NUMDAYRB2
	,CLAIMAMTRB3
	,EXPRB3
	,LIM_RB3
	,NUMDAYRB3
	,CLAIMAMTIC
	,EXPIC
	,LIM_INTERN
	,NUMDAYIC
	,CLAIMAMTIC_2
	,EXPIC_2
	,LIM_INTERN_2
	,NUMDAYIC_2
	,CLAIMAMTIC_3
	,EXPIC_3
	,LIM_INTERN_3
	,NUMDAYIC_3
	,CLAIMAMTSB
	,LIMITSB
	,CLAIMAMTSB2
	,LIMITSB2
	,CLAIMAMTSB3
	,LIMITSB3
	,CLAIMAMTSB4
	,LIMITSB4
	,CLAIMAMTSB5
	,LIMITSB5
	,CLAIMAMTSB6
	,LIMITSB6
	,CLAIMAMTSB7
	,LIMITSB7
	,CLAIMAMTSB8
	,LIMITSB8
	,CLAIMAMTSB9
	,LIMITSB9
	,CLAIMAMTSB10
	,LIMITSB10
	,CLAIMAMTIP
	,EXPIP
	,CLAIMAMTRD
	,EXPRD
	,LIM_RD
	,NUMDAYRD
	,MTHRD
	,CLAIMAMTCH
	,EXPCH
	,LIM_CHEMO
	,NUMDAYCH
	,MTHCH
	,CLAIMAMTCH2
	,EXPCH2
	,LIM_CHEMO2
	,NUMDAYCH2
	,MTHCH2
	,CLAIMAMTSR
	,EXPSR
	,LIM_SR
	,NUMDAYSR
	,ClaimAmt_RA3
	,Brachy_ext
	,ClaimAmt_RA4
	,Brachy_nonext
	,CLAIMAMTRA
	,EXPRA
	,LIM_RA
	,NUMDAYRA
	,ClaimAmt_RA2
	,Radio_super
	,CLAIMAMTER
	,EXPER
	,LIM_ERY
	,NUMDAYER
	,MTHER
	,CLAIMAMTCY
	,EXPCY
	,LIM_CYC
	,NUMDAYCY
	,MTHCY
	,CLAIMAMTIM
	,EXPIM
	,LIM_IMM
	,NUMDAYIM
	,MTHIM
	,ClaimAmtMiscarriage
	,ExpMiscarriage
	,AccumMiscarriage
	,LIM_Miscarriage
	,ClaimAmtConAB
	,ExpConAB
	,AccumConAB
	,LIM_ConAB
	,ClaimAmtPsych
	,ExpPsych
	,AccumPsych
	,LIM_Psych
	,ClaimAmtOD
	,ExpOD
	,AccumOD
	,LIM_OD
	,ClaimAmtOD_N
	,ExpOD_N
	,AccumOD_N
	,LIM_OD_N
	,ClaimAmtPros
	,ExpPros
	,AccumPros
	,LIM_Pros
	,CommClaimAmt
	,CommEXP
	,CommLim
	,CommNUMDAY
	,CLAIMAMTPPH
	,EXPPPH
	,cast(CLAIMNO as varchar(100))
	,active_record_ind
	from(select 
	dml_ind
    ,record_eff_from_date
	,TOTAL_EXP
	,EXPMISC
	,MISC_DESC
	,EXPSB
	,EXPSB2
	,EXPSB3
	,EXPSB4
	,EXPSB5
	,EXPSB6
	,EXPSB7
	,EXPSB8
	,EXPSB9
	,EXPSB10
	,CLAIMAMTRB
	,EXPRB
	,LIM_RB1
	,NUMDAYRB
	,CLAIMAMTRB2
	,EXPRB2
	,LIM_RB2
	,NUMDAYRB2
	,CLAIMAMTRB3
	,EXPRB3
	,LIM_RB3
	,NUMDAYRB3
	,CLAIMAMTIC
	,EXPIC
	,LIM_INTERN
	,NUMDAYIC
	,CLAIMAMTIC_2
	,EXPIC_2
	,LIM_INTERN_2
	,NUMDAYIC_2
	,CLAIMAMTIC_3
	,EXPIC_3
	,LIM_INTERN_3
	,NUMDAYIC_3
	,CLAIMAMTSB
	,LIMITSB
	,CLAIMAMTSB2
	,LIMITSB2
	,CLAIMAMTSB3
	,LIMITSB3
	,CLAIMAMTSB4
	,LIMITSB4
	,CLAIMAMTSB5
	,LIMITSB5
	,CLAIMAMTSB6
	,LIMITSB6
	,CLAIMAMTSB7
	,LIMITSB7
	,CLAIMAMTSB8
	,LIMITSB8
	,CLAIMAMTSB9
	,LIMITSB9
	,CLAIMAMTSB10
	,LIMITSB10
	,CLAIMAMTIP
	,EXPIP
	,CLAIMAMTRD
	,EXPRD
	,LIM_RD
	,NUMDAYRD
	,MTHRD
	,CLAIMAMTCH
	,EXPCH
	,LIM_CHEMO
	,NUMDAYCH
	,MTHCH
	,CLAIMAMTCH2
	,EXPCH2
	,LIM_CHEMO2
	,NUMDAYCH2
	,MTHCH2
	,CLAIMAMTSR
	,EXPSR
	,LIM_SR
	,NUMDAYSR
	,ClaimAmt_RA3
	,Brachy_ext
	,ClaimAmt_RA4
	,Brachy_nonext
	,CLAIMAMTRA
	,EXPRA
	,LIM_RA
	,NUMDAYRA
	,ClaimAmt_RA2
	,Radio_super
	,CLAIMAMTER
	,EXPER
	,LIM_ERY
	,NUMDAYER
	,MTHER
	,CLAIMAMTCY
	,EXPCY
	,LIM_CYC
	,NUMDAYCY
	,MTHCY
	,CLAIMAMTIM
	,EXPIM
	,LIM_IMM
	,NUMDAYIM
	,MTHIM
	,ClaimAmtMiscarriage
	,ExpMiscarriage
	,AccumMiscarriage
	,LIM_Miscarriage
	,ClaimAmtConAB
	,ExpConAB
	,AccumConAB
	,LIM_ConAB
	,ClaimAmtPsych
	,ExpPsych
	,AccumPsych
	,LIM_Psych
	,ClaimAmtOD
	,ExpOD
	,AccumOD
	,LIM_OD
	,ClaimAmtOD_N
	,ExpOD_N
	,AccumOD_N
	,LIM_OD_N
	,ClaimAmtPros
	,ExpPros
	,AccumPros
	,LIM_Pros
	,CommClaimAmt
	,CommEXP
	,CommLim
	,CommNUMDAY
	,CLAIMAMTPPH
	,EXPPPH
	,CLAIMNO
	,active_record_ind
	,row_number() over(partition by business_key order by coalesce(change_seq,-1) desc, record_eff_from_date desc, record_eff_to_date desc) rnk
from tl_wbcs_def.tb_isclaimsdetails_hist)
	where rnk=1;	
	
CREATE TABLE #tb_pmiadv_main_hist AS 
SELECT  
     dml_ind
    ,record_eff_from_date
    ,LIABILITY
	,cast(CLAIMNO as varchar(100))
	,active_record_ind
	from(select
	dml_ind
    ,record_eff_from_date
    ,LIABILITY
	,CLAIMNO
	,active_record_ind
	,row_number() over(partition by business_key order by coalesce(change_seq,-1) desc, record_eff_from_date desc, record_eff_to_date desc) rnk
	from tl_wbcs_def.tb_pmiadv_main_hist)
	where rnk=1; 
	
	
CREATE TABLE #tb_DiagnosisCode_hist AS 
SELECT  
     dml_ind
    ,record_eff_from_date
    ,Description
	,ICDType
	,Code
	,active_record_ind
	from 
	(select 
	dml_ind
    ,record_eff_from_date
    ,Description
	,ICDType
	,Code
	,active_record_ind
	,row_number() over(partition by Code order by coalesce(change_seq,-1) desc, record_eff_from_date desc, record_eff_to_date desc) rnk	
	from
	tl_wbcs_def.tb_DiagnosisCode_hist)
	where rnk=1; 
	
CREATE TABLE #tb_issubclaims_hist AS 
SELECT  
     dml_ind
     ,record_eff_from_date
     ,cast(ClaimNo as varchar(100))
	 ,PolicyNo
	 ,PolicyID
	 ,active_record_ind
	from(select
	 dml_ind
     ,record_eff_from_date
     ,ClaimNo
	 ,PolicyNo
	 ,PolicyID
	 ,active_record_ind
	 ,row_number() over(partition by business_key order by coalesce(change_seq,-1) desc, record_eff_from_date desc, record_eff_to_date desc) rnk
	from tl_wbcs_def.tb_issubclaims_hist)
	where rnk=1; 
	

CREATE TABLE #ISSubClaims
	(
		ClaimNo VARCHAR(60)
		,PolicyNo VARCHAR(50)
		,PolicyID BIGINT
		,record_eff_from_date timestamp
	);

	--Sample Production Data: RSRpt.WBCS.ISSubClaims ClaimNo like '9876918%'
	--ClaimNo 9876918H and 9876918C
	INSERT INTO #ISSubClaims
	SELECT ClaimNo,PolicyNo,PolicyID,record_eff_from_date  
	FROM
		(
			SELECT DISTINCT 
				REPLACE (REPLACE(Claimno,'H',''),'C','') as ClaimNo
				,PolicyNo
				,PolicyID
				,record_eff_from_date
			FROM #tb_issubclaims_hist where active_record_ind='Y'
		) t;
	
	
CREATE TABLE #PKPrimary_stg
AS 
SELECT Claimno,record_eff_from_date
FROM
(select Claimno, c.record_eff_from_date from #tb_claims_hist c WHERE c.record_eff_from_date >=(SELECT v_vLastRunDate FROM #v_rundate) 
Union
select Claimno, isc.record_eff_from_date from #tb_isclaims_hist isc WHERE isc.record_eff_from_date >=(SELECT v_vLastRunDate FROM #v_rundate)
Union
select Claimno, icd.record_eff_from_date from #tb_isclaimsdetails_hist icd WHERE icd.record_eff_from_date >=(SELECT v_vLastRunDate FROM #v_rundate)
Union
select Claimno , pm.record_eff_from_date from #tb_pmiadv_main_hist pm WHERE pm.record_eff_from_date >=(SELECT v_vLastRunDate FROM #v_rundate)
Union
select DISTINCT REPLACE (REPLACE(Claimno,'H',''),'C','') as ClaimNo, iss.record_eff_from_date from #tb_issubclaims_hist iss WHERE iss.record_eff_from_date >=(SELECT v_vLastRunDate FROM #v_rundate)
Union
select c.Claimno, subdc.record_eff_from_date from #tb_claims_hist c inner join #tb_isclaims_hist isc on c.ClaimNo = isc.CLAIMNO inner join #tb_DiagnosisCode_hist subdc on subdc.Code = (CASE WHEN POSITION(' ' IN RTRIM(isc.MEDICODE))>0 THEN SUBSTRING(RTRIM(isc.MEDICODE),1,POSITION(' ' in RTRIM(isc.MEDICODE))-1) ELSE RTRIM(isc.MEDICODE) END ) WHERE subdc.record_eff_from_date >=(SELECT v_vLastRunDate FROM #v_rundate)
);

CREATE TABLE #PKPrimary AS
Select Claimno, record_eff_from_date
FROM(Select
Claimno,
record_eff_from_date,
row_number() over( partition by Claimno order by record_eff_from_date desc ) rnk
from #PKPrimary_stg )
where rnk=1;


DELETE FROM el_eds_def_stg.tempstgdimisclaim WHERE source_app_code='WBCS';

INSERT into el_eds_def_stg.tempstgdimisclaim
		(source_app_code
		,source_data_set
		,dml_ind
		,record_created_date
		,record_updated_date
		,record_created_by
		,record_updated_by
		,record_eff_from_date
		,record_eff_to_date
		,active_record_ind
		,claim_uuid
		,business_key
		,claim_event_no
		,claim_no
		,claim_event_occurance_date
		,claim_event_type_code
		,claim_event_type_id
		,claim_type_code
		,claim_type_id
		,claim_event_status_code
		,claim_event_status_id
		,claim_status_code
		,Claim_status_id
		,claim_event_closed_date
		,claim_closed_date
		,claim_reported_date
		,claim_description
		,policy_no
		,policy_uuid
		,customer_uuid
		,product_uuid
		,sub_product_uuid
		,sales_agent_uuid
		,sales_agent_code
		,servicing_agent_uuid
		,amount_claimed
		,ward
		,hospital_id
		,hospital_type
		,hospital_name
		,hospital_admission_date
		,hospital_discharge_date
		,specialist_name
		,speciality
		,primary_diagnosis_code
		,primary_diagnosis_uuid
		,primary_diagnosis_name
		,secondary_diagnosis_code
		,secondary_diagnosis_uuid
		,secondary_diagnosis_name
		,icd_type_code
		,pro_ration_percent
		,claim_billed_amount
		,third_party_payment_amount
		,claim_co_pay_amount
		,net_payable_main_plan
		,net_payable_rider_plan
		,expense_misc
		,misc_description
		,claim_deductible_amount
		,claim_plus_rider_deductible_amount
		,claim_assist_rider_deductible_amount
		,claim_payment_date
		,gross_claim_paid_with_gst
		,gross_claim_incurred_with_gst
		,gross_claim_incurred_without_gst
		,sb1op_code
		,sb2op_code
		,sb3op_code
		,sb4op_code
		,sb5op_code
		,sb6op_code
		,sb7op_code
		,sb8op_code
		,sb9op_code
		,sb10op_code
		,expsb
		,expsb2
		,expsb3
		,expsb4
		,expsb5
		,expsb6
		,expsb7
		,expsb8
		,expsb9
		,expsb10
		,claimamtrb
		,exprb
		,lim_rb1
		,numdayrb
		,rm_bdate_from
		,rm_bdate_to
		,claimamtrb2
		,exprb2
		,lim_rb2
		,numdayrb2
		,rm2_bdate_from
		,rm2_bdate_to
		,claimamtrb3
		,exprb3
		,lim_rb3
		,numdayrb3
		,rm3_bdate_from
		,rm3_bdate_to
		,claimamtic
		,expic
		,lim_intern
		,numdayic
		,claimamtic_2
		,expic_2
		,lim_intern_2
		,numdayic_2
		,claimamtic_3
		,expic_3
		,lim_intern_3
		,numdayic_3
		,claimamtsb
		,limitsb
		,claimamtsb2
		,limitsb2
		,claimamtsb3
		,limitsb3
		,claimamtsb4
		,limitsb4
		,claimamtsb5
		,limitsb5
		,claimamtsb6
		,limitsb6
		,claimamtsb7
		,limitsb7
		,claimamtsb8
		,limitsb8
		,claimamtsb9
		,limitsb9
		,claimamtsb10
		,limitsb10
		,claimamtip
		,expip
		,claimamtrd
		,exprd
		,lim_rd
		,numdayrd
		,mthrd
		,claimamtch
		,expch
		,lim_chemo
		,numdaych
		,mthch
		,claimamtsr
		,expsr
		,lim_sr
		,numdaysr
		,claim_amt_ra3
		,brachy_ext
		,claim_amt_ra4
		,brachy_nonext
		,claimamtra
		,expra
		,lim_ra
		,numdayra
		,claim_amt_ra2
		,radio_super
		,claimamter
		,exper
		,lim_ery
		,numdayer
		,mther
		,claimamtcy
		,expcy
		,lim_cyc
		,numdaycy
		,mthcy
		,claimamtim
		,expim
		,lim_imm
		,numdayim
		,mthim
		,claim_amt_miscarriage
		,exp_miscarriage
		,accum_miscarriage
		,lim_miscarriage
		,claim_amt_con_ab
		,exp_con_ab
		,accum_con_ab
		,lim_con_ab
		,claim_amt_psych
		,exp_psych
		,accum_psych
		,lim_psych
		,claim_amt_od
		,exp_od
		,accum_od
		,lim_od
		,claim_amt_od_n
		,exp_od_n
		,accum_od_n
		,lim_od_n
		,claim_amt_pros
		,exp_pros
		,accum_pros
		,lim_pros
		,comm_claim_amt
		,comm_exp
		,comm_lim
		,comm_numday
		,claimamtpph
		,exppph
		,hcbvalue
		,policy_id
		,claim_submission_type
		,claim_type)
		SELECT    
			'WBCS'as source_app_code 
			,'WBCS-ISIS' as source_data_set
			,c.dml_ind as dml_ind
			,getdate() as record_created_date
			,getdate() as record_updated_date
			,'EDS' as record_created_by 
			,'EDS' as record_updated_by 
			,pk.record_eff_from_date as record_eff_from_date
			,cast('9999-12-31 00:00:00.000000' as timestamp) as record_eff_to_date
			,'Y' as active_record_ind
			,sha2('WBCS' || '~' || c.ClaimNo,256) AS claim_uuid
			,('WBCS' || '~' || c.ClaimNo) AS business_key 
			,c.ClaimNo  as Claim_event_no
					, c.ClaimNo  as Claim_no
					, c.DateOcc as Claim_event_occurance_date
					, isc.BillCategory as Claim_event_type_code
					, ISNULL(dcetm.ref_claim_event_type_id, -1) as Claim_event_type_id
					, isc.BillCategory as Claim_type_code
					, ISNULL(dctm.ref_claim_type_id, -1) as Claim_type_id
					, c.Status as Claim_event_status_code
					, ISNULL(dcesm.ref_claim_event_status_id, -1) as Claim_event_status_id
					, c.Status as Claim_status_code
					, ISNULL(dcsm.ref_claim_status_id, -1) as Claim_status_id
					, c.DateClosed  as Claim_event_closed_date
					, c.DateClosed  as Claim_closed_date
					, c.DateClaim  as Claim_reported_date
					, c.ClaimDesc as Claim_description
					, CASE WHEN UPPER(c.ClaimType) = 'IS' THEN c.PolicyNo WHEN UPPER(c.ClaimType) = 'ISX' THEN sc.PolicyNo ELSE NULL END as policy_no
					, ISNULL(dp.policy_uuid,'-1') as policy_uuid
					, ISNULL(dp.customer_uuid,'-1') as customer_uuid
					, ISNULL(dp.product_uuid,'-1') as product_uuid
					, ISNULL(dp.sub_product_uuid,'-1') as sub_product_uuid
					, ISNULL(dp.sales_agent_uuid,'-1') as sales_agent_uuid
					, ISNULL(da.Agent_Code,'UNKNOWN') as sales_agent_code
					, ISNULL(dp.servicing_agent_uuid,'-1') as servicing_agent_uuid
					--FactAmountClaim
					,ISNULL(c.AmtClaimed, 0) as Amount_claimed
					,isc.warddesc as Ward
				    ,isc.HospCode as hospital_id
					,CASE 
										WHEN UPPER(isc.HOSTYPE) = 'C' THEN 'Community'
										WHEN UPPER(isc.HOSTYPE) = 'G' THEN 'Government'
										WHEN UPPER(isc.HOSTYPE) = 'S' THEN 'Restructured'
										WHEN UPPER(isc.HOSTYPE) = 'V' THEN 'Private'	  
									END as hospital_type
					, --  hac.HospName 
					  isc.HospCodedesc as hospital_name
					, isc.RM_BDATE_FROM as hospital_admission_date
					, isc.RM_BDATE_TO as hospital_discharge_date
					, isc.SPECIALIST as specialist_name
					, isc.SpecialityDesc as speciality
					, (CASE WHEN position(' ' in RTRIM(isc.MEDICODE))>0 THEN SUBSTRING(RTRIM(isc.MEDICODE),1,position(' ' in RTRIM(isc.MEDICODE))-1) ELSE RTRIM(isc.MEDICODE) END )  as primary_diagnosis_code 
					, ISNULL(dd.Diagnosis_uuid, '-1') as primary_diagnosis_uuid
					, subdc.Description as primary_diagnosis_name
					,(CASE WHEN position(' ' in RTRIM(isc.OtherDiagnosis))>0 THEN SUBSTRING(RTRIM(isc.OtherDiagnosis),1,position(' ' in RTRIM(isc.OtherDiagnosis))-1) ELSE RTRIM(isc.OtherDiagnosis) END )  as secondary_diagnosis_code
					, ISNULL(dd2.Diagnosis_uuid, '-1') as secondary_diagnosis_uuid
					, subdc2.Description as secondary_diagnosis_name
					,cast(subdc.ICDType as INTEGER )as icd_type_code
					, isc.ProRationPercent as pro_ration_percent
					--FactISClaim
					,ISNULL(iscd.TOTAL_EXP, 0) as Claim_billed_amount--ISNULL(c.AmtClaimed, 0)
					,ISNULL(isc.AMTPAID_TP, 0) as Third_party_payment_amount
	
					,ISNULL(isc.COINSURANCE, 0) as Claim_co_pay_amount 
	
					,ISNULL(
					(
						CASE 
						WHEN (isc.HRN = '' OR isc.HRN IS NULL)
						THEN ((isc.Total_IS_Pays - 
								CASE 
									WHEN REGEXP_INSTR(TRIM(isc.HCBValue), '[0-9]') > 0 THEN 
										CAST(REPLACE(isc.HCBValue, ',', '')AS decimal(32,6))
									ELSE 0 
								END - 
								COALESCE(isc.EBBClaimAmt, 0))
							)
						ELSE (ISNULL(pm.LIABILITY,0) - ISNULL(isc.EBBClaimAmt,0)) END  --TotalAmountPayable
						-
						--AmountPayableRider
						 CASE 
						   WHEN isc.AMTPAID_TP > 0 AND CASE WHEN REGEXP_INSTR(TRIM(COALESCE(isc.HCBValue, '0')), '[0-9]') > 0 
								AND isc.HCBValue ~ '^[0-9\.]+$' THEN 
									CAST(REPLACE(isc.HCBValue, ',', '')AS decimal(32,6))
								ELSE 0 
							END > 0
						  THEN
							CASE 
								WHEN (COALESCE(isc.RiderWaiver, 0) - 
									 (CASE 
										 WHEN REGEXP_INSTR(TRIM(COALESCE(isc.HCBValue, '0')), '[0-9]') > 0 
										 AND isc.HCBValue ~ '^[0-9\.]+$' THEN 
											 CAST(REPLACE(isc.HCBValue, ',', '')AS decimal(32,6))
										 ELSE 0 
									 END)) < 0 
								THEN 0
								ELSE (COALESCE(isc.RiderWaiver, 0) - 
									 (CASE 
										 WHEN REGEXP_INSTR(TRIM(COALESCE(isc.HCBValue, '0')), '[0-9]') > 0 
										 AND isc.HCBValue ~ '^[0-9\.]+$' THEN 
											 CAST(REPLACE(isc.HCBValue, ',', '')AS decimal(32,6))
										 ELSE 0 
									 END))
							END
						 ELSE
						 isc.RiderWaiver
						  END
						),0) as Net_payable_main_plan

		, ISNULL(
					CASE WHEN (isc.PayablePolicyRider = '' OR isc.PayablePolicyRider IS NULL OR  isc.PayablePolicyRider='NA') AND (isc.RiderPrdtCode IS NULL OR isc.RiderPrdtCode='')  THEN  0
                    ELSE
                                                                           CASE 
                      WHEN isc.AMTPAID_TP > 0 AND CASE 
												WHEN REGEXP_INSTR(TRIM(isc.HCBValue), '[0-9]') > 0 THEN 
													CAST(REPLACE(isc.HCBValue, ',', '')AS decimal(32,6))
												ELSE 0 
											END > 0
                        THEN
						CASE WHEN (COALESCE(isc.RiderWaiver, 0) - 
								 (CASE 
									 WHEN REGEXP_INSTR(TRIM(COALESCE(isc.HCBValue, '0')), '[0-9]') > 0 
									 AND isc.HCBValue ~ '^[0-9\.]+$' THEN 
										 CAST(REPLACE(isc.HCBValue, ',', '')AS decimal(32,6))
									 ELSE 0 
								 END)) < 0 
							THEN 0
							ELSE (COALESCE(isc.RiderWaiver, 0) - 
								 (CASE 
									 WHEN REGEXP_INSTR(TRIM(COALESCE(isc.HCBValue, '0')), '[0-9]') > 0 
									 AND isc.HCBValue ~ '^[0-9\.]+$' THEN 
										 CAST(REPLACE(isc.HCBValue, ',', '')AS decimal(32,6))
									 ELSE 0 
								 END))
						END
                     ELSE
                      isc.RiderWaiver
                     END                       
                   END
				,0) as Net_payable_rider_plan

				, ISNULL(iscd.EXPMISC, 0) as expense_misc
				, iscd.MISC_DESC as misc_description
				, ISNULL(isc.DEDUCTIBLES, 0)  as claim_deductible_amount
				, ISNULL(isc.COINSURANCE, 0) as claim_plus_rider_deductible_amount
				, ISNULL(isc.AssR_CoInsure, 0) as claim_assist_rider_deductible_amount

					--,[OtherExcludedBenefitAmount] = ISNULL(iscd.EXPMISC, 0)

					--FactClaimPayment
					, c.DateClosed  as claim_payment_date
					, ISNULL(c.AmtPaid, 0) as gross_claim_paid_with_gst

	
					--FactISClaimBalance
					--,[PolicyYearBalance] = ISNULL(isc.BAL_POLICY_YR, 0)
					--,[LifeTimeBalance] = ISNULL(isc.BAL_LIFETIME, 0)

					--FactClaimReserved
					, ISNULL(c.Reserve, 0)  as gross_claim_incurred_with_gst
					, ISNULL(c.Reserve, 0) as gross_claim_incurred_without_gst
					--FactClaimOperation
					, isc.SB1OpCode as sb1op_code
					, isc.SB2OpCode as sb2op_code
					, isc.SB3OpCode as sb3op_code
					, isc.SB4OpCode as sb4op_code
					, isc.SB5OpCode as sb5op_code
					, isc.SB6OpCode as sb6op_code
					, isc.SB7OpCode as sb7op_code
					, isc.SB8OpCode as sb8op_code
					, isc.SB9OpCode as sb9op_code
					, isc.SB10OpCode as sb10op_code
					, iscd.EXPSB as expsb
					, iscd.EXPSB2 as expsb2
					, iscd.EXPSB3 as expsb3
					, iscd.EXPSB4 as expsb4
					, iscd.EXPSB5 as expsb5
					, iscd.EXPSB6 as expsb6
					, iscd.EXPSB7 as expsb7
					, iscd.EXPSB8 as expsb8
					, iscd.EXPSB9 as expsb9
					, iscd.EXPSB10 as expsb10
					--FactClaimBenefit
					-- Room 1
					, iscd.CLAIMAMTRB as claimamtrb
					, iscd.EXPRB as exprb
					, iscd.LIM_RB1 as lim_rb1
					, iscd.NUMDAYRB as numdayrb
					, isc.RM_BDATE_FROM as rm_bdate_from
					, isc.RM_BDATE_TO as rm_bdate_to	
					-- Room 2
					, iscd.CLAIMAMTRB2 as claimamtrb2
					, iscd.EXPRB2 as exprb2
					, iscd.LIM_RB2 as lim_rb2
					, iscd.NUMDAYRB2 as numdayrb2
					, isc.RM2_BDATE_FROM as rm2_bdate_from
					, isc.RM2_BDATE_TO as rm2_bdate_to
					-- Room 3
					, iscd.CLAIMAMTRB3 as claimamtrb3
					, iscd.EXPRB3 as exprb3
					, iscd.LIM_RB3 as lim_rb3
					, iscd.NUMDAYRB3 as numdayrb3
					, isc.RM3_BDATE_FROM as rm3_bdate_from
					, isc.RM3_BDATE_TO as rm3_bdate_to
					-- ICU 1
					, iscd.CLAIMAMTIC as claimamtic
					, iscd.EXPIC as expic
					, iscd.LIM_INTERN as lim_intern
					, iscd.NUMDAYIC as numdayic
					-- ICU 2
					, iscd.CLAIMAMTIC_2 as claimamtic_2
					, iscd.EXPIC_2 as expic_2
					, iscd.LIM_INTERN_2 as lim_intern_2
					, iscd.NUMDAYIC_2 as numdayic_2
					-- ICU 3
					, iscd.CLAIMAMTIC_3 as claimamtic_3
					, iscd.EXPIC_3 as expic_3
					, iscd.LIM_INTERN_3 as lim_intern_3
					, iscd.NUMDAYIC_3 as numdayic_3
					-- SURGERY LIMITS TABLE 1
					, iscd.CLAIMAMTSB as claimamtsb
					--iscd.EXPSB
					, iscd.LIMITSB as limitsb
					-- SURGERY LIMITS TABLE 2
					, iscd.CLAIMAMTSB2 as claimamtsb2
					--iscd.EXPSB2
					, iscd.LIMITSB2 as limitsb2
					-- SURGERY LIMITS TABLE 3
					, iscd.CLAIMAMTSB3 as claimamtsb3
					--iscd.EXPSB3
					, iscd.LIMITSB3 as limitsb3
					-- SURGERY LIMITS TABLE 4
					, iscd.CLAIMAMTSB4 as claimamtsb4
					--iscd.EXPSB4
					, iscd.LIMITSB4 as limitsb4
					-- SURGERY LIMITS TABLE 5
					, iscd.CLAIMAMTSB5 as claimamtsb5
					--iscd.EXPSB5
					, iscd.LIMITSB5 as limitsb5
					-- SURGERY LIMITS TABLE 6
					, iscd.CLAIMAMTSB6 as claimamtsb6
					--iscd.EXPSB6
					, iscd.LIMITSB6 as limitsb6
					-- SURGERY LIMITS TABLE 7
					, iscd.CLAIMAMTSB7 as claimamtsb7
					--iscd.EXPSB7
					, iscd.LIMITSB7 as limitsb7
					-- SURGERY LIMITS TABLE 8
					, iscd.CLAIMAMTSB8 as claimamtsb8
					--iscd.EXPSB8
					, iscd.LIMITSB8 as limitsb8
					-- SURGERY LIMITS TABLE 9
					, iscd.CLAIMAMTSB9 as claimamtsb9
					--iscd.EXPSB9
					, iscd.LIMITSB9 as limitsb9
					-- SURGERY LIMITS TABLE 10
					, iscd.CLAIMAMTSB10 as claimamtsb10
					--iscd.EXPSB10
					, iscd.LIMITSB10 as limitsb10
					--SURGICAL IMPLANTS
					, iscd.CLAIMAMTIP as claimamtip
					, iscd.EXPIP as expip
					--RENAL DIALYSIS
					, iscd.CLAIMAMTRD as claimamtrd
					, iscd.EXPRD as exprd
					, iscd.LIM_RD as lim_rd
					, iscd.NUMDAYRD as numdayrd
					, iscd.MTHRD as mthrd
					--CHEMOTHERAPY MONTHLY
					, iscd.CLAIMAMTCH as claimamtch
					, iscd.EXPCH as expch
					, iscd.LIM_CHEMO as lim_chemo
					, iscd.NUMDAYCH as numdaych
					, iscd.MTHCH as mthch
					---- CHEMOTHERAPY WEEKLY
					--,[CLAIMAMTCH2] = iscd.CLAIMAMTCH2
					--,[EXPCH2] = iscd.EXPCH2
					--,[LIM_CHEMO2] = iscd.LIM_CHEMO2
					--,[NUMDAYCH2] = iscd.NUMDAYCH2
					--,[MTHCH2] = iscd.MTHCH2
					-- STEREOTACTIC RADIOTHERAPY 
					, iscd.CLAIMAMTSR as claimamtsr
					, iscd.EXPSR as expsr
					, iscd.LIM_SR as lim_sr
					, iscd.NUMDAYSR as numdaysr
					--RADIOTHERAPY - BRACHYTHERAPY WITH EXTERNAL
					, iscd.ClaimAmt_RA3 as claim_amt_ra3
					, iscd.Brachy_ext as brachy_ext
					--RADIOTHERAPY - BRACHYTHERAPY WITHOUT EXTERNAL
					, iscd.ClaimAmt_RA4 as claim_amt_ra4
					, iscd.Brachy_nonext as brachy_nonext
					--RADIOTHERAPY
					, iscd.CLAIMAMTRA as claimamtra
					, iscd.EXPRA as expra
					, iscd.LIM_RA as lim_ra
					, iscd.NUMDAYRA as numdayra
					--RADIOTHERAPY SUPERFICIAL
					, iscd.ClaimAmt_RA2 as claim_amt_ra2
					, iscd.Radio_super as radio_super
					--ERYTHROPOIETIN
					, iscd.CLAIMAMTER as claimamter
					, iscd.EXPER as exper
					, iscd.LIM_ERY as lim_ery
					, iscd.NUMDAYER as numdayer
					, iscd.MTHER as mther
					--CYCLOSPORIN
					, iscd.CLAIMAMTCY as claimamtcy
					, iscd.EXPCY as expcy
					, iscd.LIM_CYC as lim_cyc
					, iscd.NUMDAYCY as numdaycy
					, iscd.MTHCY as mthcy
					--IMMUNOTHERAPY
					, iscd.CLAIMAMTIM as claimamtim
					, iscd.EXPIM as expim
					, iscd.LIM_IMM as lim_imm
					, iscd.NUMDAYIM as numdayim
					, iscd.MTHIM as mthim
					--PREGNANCY COMPLICATION BENEFIT
					, iscd.ClaimAmtMiscarriage as claim_amt_miscarriage
					, iscd.ExpMiscarriage as exp_miscarriage
					, iscd.AccumMiscarriage as accum_miscarriage
					, iscd.LIM_Miscarriage as lim_miscarriage
					--CONGENITAL ABNORMALITIES BENEFIT
					, iscd.ClaimAmtConAB as claim_amt_con_ab
					, iscd.ExpConAB as exp_con_ab
					, iscd.AccumConAB as accum_con_ab
					, iscd.LIM_ConAB as lim_con_ab
					--INPATIENT PSYCHIATRIC TREATMENT
					, iscd.ClaimAmtPsych as claim_amt_psych
					, iscd.ExpPsych as exp_psych
					, iscd.AccumPsych as accum_psych
					, iscd.LIM_Psych as lim_psych
					--LIVING ORGAN DONOR (INSURED) TRANSPLANT
					, iscd.ClaimAmtOD as claim_amt_od
					, iscd.ExpOD as exp_od
					, iscd.AccumOD as accum_od
					, iscd.LIM_OD as lim_od
					--LIVING ORGAN DONOR (NON INSURED) TRANSPLANT
					, iscd.ClaimAmtOD_N as claim_amt_od_n
					, iscd.ExpOD_N as exp_od_n
					, iscd.AccumOD_N as accum_od_n
					, iscd.LIM_OD_N as lim_od_n
					--PROSTHESIS
					, iscd.ClaimAmtPros as claim_amt_pros
					, iscd.ExpPros as exp_pros
					, iscd.AccumPros as accum_pros
					, iscd.LIM_Pros as lim_pros
					--COMMUNITY HOSP RB
					, iscd.CommClaimAmt as comm_claim_amt
					, iscd.CommEXP as comm_exp
					, iscd.CommLim as comm_lim
					, iscd.CommNUMDAY as comm_numday
					--PRE POST HOSPITAL -- PPH
					, iscd.CLAIMAMTPPH as claimamtpph
					, iscd.EXPPPH as exppph
					--HOSPITAL CASH BENEFIT -- HCB
					,CASE 
						WHEN REGEXP_INSTR(TRIM(isc.HCBValue), '[0-9]') > 0 THEN 
							--CAST(isc.HCBValue AS decimal(32,6))
							CAST(REPLACE(isc.HCBValue, ',', '')AS decimal(32,6))
						ELSE 0 
					END as hcbvalue
					,CASE WHEN ISNULL(c.PolicyID,0)=0 THEN dc.Policy_ID ELSE c.PolicyID END as policy_id--c.PolicyId
					, isc.Sub_Type as claim_submission_type
					,c.ClaimType  as claim_type
		FROM #PKPrimary pk inner join #tb_claims_hist c on pk.claimno=c.Claimno 
		LEFT OUTER JOIN #tb_isclaims_hist isc 
			ON c.ClaimNo = isc.CLAIMNO and isc.active_record_ind='Y'
		LEFT JOIN #tb_isclaimsdetails_hist iscd 
			ON c.CLAIMNO = iscd.CLAIMNO and iscd.active_record_ind='Y'
		LEFT JOIN #tb_pmiadv_main_hist pm 
			ON c.CLAIMNO = pm.CLAIMNO and pm.active_record_ind='Y'
		/*   commented below as wardclassname and hospname referred from tb_isclaims_hist
		LEFT JOIN #tb_ISWardClass_hist iswc 
			ON isc.Ward = iswc.WardClassID
		LEFT JOIN (SELECT DISTINCT HRNprefix, HospName FROM #tb_HospAddrCode_hist) hac 
			ON isc.HospCode = hac.HRNprefix */
		--PrimaryDiagnosis
		LEFT JOIN
			/* Handled in CREATE #Table 
			SELECT RN = ROW_NUMBER() OVER (PARTITION BY Code ORDER BY Effdate DESC), * 
			FROM */
			#tb_DiagnosisCode_hist subdc 
			ON subdc.Code = (CASE WHEN POSITION(' ' IN RTRIM(isc.MEDICODE))>0 THEN UPPER(SUBSTRING(RTRIM(isc.MEDICODE),1,POSITION(' ' in RTRIM(isc.MEDICODE))-1)) ELSE UPPER(RTRIM(isc.MEDICODE)) END ) and subdc.active_record_ind='Y' and isc.active_record_ind='Y' 
		--SecondaryDiagnosis
		LEFT JOIN 
			/*Handled in CREATE #Table 
			SELECT RN = ROW_NUMBER() OVER (PARTITION BY Code ORDER BY Effdate DESC), * 
			FROM */	
			#tb_DiagnosisCode_hist subdc2 
			ON subdc2.Code = (CASE WHEN POSITION(' 'IN RTRIM(isc.OtherDiagnosis))>0 THEN UPPER(SUBSTRING(RTRIM(isc.OtherDiagnosis),1,POSITION(' ' in RTRIM(isc.OtherDiagnosis))-1)) ELSE UPPER(RTRIM(isc.OtherDiagnosis)) END ) and subdc2.active_record_ind='Y' and isc.active_record_ind='Y'		  
		LEFT JOIN #DimClaimEventTypeMapping dcetm
			ON UPPER(isc.BillCategory) = dcetm.claim_event_type_code 
		LEFT JOIN #DimClaimTypeMapping dctm
			ON UPPER(isc.BillCategory) = dctm.Claim_Type_Code 
		LEFT JOIN #DimClaimEventStatusMapping dcesm
			ON UPPER(c.Status) = dcesm.Claim_Event_Status_Code 
		LEFT JOIN #DimClaimStatusMapping dcsm
			ON UPPER(c.Status) = dcsm.Claim_Status_Code 
		LEFT JOIN #DimDiagnosis dd
			ON (CASE WHEN position(' 'in RTRIM(isc.MEDICODE))>0 THEN UPPER(SUBSTRING(RTRIM(isc.MEDICODE),1,position(' ' in RTRIM(isc.MEDICODE))-1)) ELSE UPPER(RTRIM(isc.MEDICODE)) END )
			= dd.Diagnosis_Code and isc.active_record_ind='Y'
			--AND c.DateOcc BETWEEN dd.Diagnosis_Effective_Date AND dd.Diagnosis_Expiry_Date
		LEFT JOIN #DimDiagnosis dd2
			ON (CASE WHEN position(' 'in RTRIM(isc.OtherDiagnosis))>0 THEN UPPER(SUBSTRING(RTRIM(isc.OtherDiagnosis),1,position(' ' in RTRIM(isc.OtherDiagnosis))-1)) ELSE UPPER(RTRIM(isc.OtherDiagnosis)) END ) 
			= dd2.Diagnosis_Code  and isc.active_record_ind='Y'
		--AND c.DateOcc BETWEEN dd2.Diagnosis_Effective_Date AND dd2.Diagnosis_Expiry_Date
		
		--For Incremental Policy
		LEFT JOIN #DimClaim dc
			ON c.ClaimNo = dc.Claim_No AND c.PolicyNo = dc.Policy_No 
		LEFT JOIN #DimPolicy dp
			ON CASE WHEN ISNULL(c.PolicyID,0)=0 THEN dc.Policy_ID ELSE c.PolicyID END = dp.Policy_ID 
		LEFT JOIN #ISSubClaims sc 
			ON sc.Claimno = c.ClaimNo
		LEFT JOIN #DimAgent da
			ON dp.Sales_Agent_uuid = da.Agent_uuid
		WHERE upper(c.ClaimType) IN ('IS','ISX');

SELECT t.Claim_No, dp3.Policy_uuid INTO #temp
	FROM (
		SELECT 
		a.Claim_No 
		,MAX(COALESCE(dp.Policy_id, dp2.Policy_id, -1)) AS Policy_id
		FROM el_eds_def_stg.tempstgdimisclaim  a
		LEFT JOIN #DimPolicy dp
			ON a.Policy_No = dp.Policy_No
			AND a.claim_event_occurance_date  BETWEEN dp.Policy_Start_Date AND dp.Policy_End_Date
		LEFT JOIN (
			SELECT Policy_No, MIN(Policy_id) AS Policy_id
			FROM #DimPolicy 
		GROUP BY Policy_No
	) dp2 ON a.Policy_No = dp2.Policy_No 
		GROUP BY a.Claim_No
	) t
	LEFT JOIN #DimPolicy dp3
	ON t.Policy_id = dp3.Policy_id;
	
	--/********* Start: PolicySeqID population For ClaimType = 'ISX' *********/

	----RSRpt.WBCS.ISSubClaims stores both IS and ISX so need to filter ISX

	SELECT a.Claim_No
	,ISNULL(dp3.Policy_uuid, '-1') as Policy_uuid
	,sc.PolicyNo
	INTO #ISX
	FROM #tempstgDimISClaim a
	INNER JOIN #ISSubClaims sc
	ON sc.Claimno  = a.Claim_No 
	LEFT JOIN #DimPolicy dp3
	ON cast(sc.PolicyID as int)= dp3.Policy_ID 
	WHERE UPPER(a.Claim_Type) = 'ISX';
	
	----#temp stores both IS and ISX
	----this update script will update only ClaimType ISX becasue #ISX stores only ISX

	UPDATE a
	SET Policy_uuid = b.Policy_uuid
	FROM #temp a, #ISX b
	WHERE a.Claim_No = b.Claim_No;

	--/********* End: PolicySeqID population For ClaimType = 'ISX' *********/

	UPDATE el_eds_def_stg.tempstgdimisclaim as a 
	SET
		policy_uuid = ISNULL(dp.policy_uuid, '-1')  
		,customer_uuid = ISNULL(dp.customer_uuid, '-1')  
		,product_uuid = ISNULL(dp.product_uuid, '-1')  
		,sub_product_uuid = ISNULL(dp.Sub_product_uuid, '-1')  
		,sales_agent_uuid = ISNULL(dp.sales_agent_uuid, '-1')  
		,Sales_Agent_Code = ISNULL(da.Agent_Code,'UNKNOWN')  
		,servicing_agent_uuid = ISNULL(dp.servicing_agent_uuid, '-1')  
		,Policy_Id = dp.Policy_Id
	--If PolicyNo column is not updated here, ISX records' PolicyNo will be having NULL
		,Policy_No = CASE WHEN UPPER(a.Claim_Type) = 'IS' 
				THEN a.Policy_No 
				ELSE dp.Policy_No --ISX
		END  
		from #temp b
		LEFT JOIN #DimPolicy dp
		ON b.policy_uuid = dp.policy_uuid
		LEFT JOIN #DimAgent da
		ON dp.sales_agent_uuid = da.agent_uuid where a.Claim_No = b.Claim_No;
		
	UPDATE el_eds_def_stg.tempstgdimisclaim
		SET dml_ind= case when a.dml_ind<>'D' then 'U' else a.dml_ind end
		FROM el_eds_def_stg.tempstgdimisclaim a inner join el_eds_def.dimclaim b on a.business_key = b.business_key
		WHERE  b.active_record_ind='Y'
		AND a.source_app_code='WBCS' and b.source_app_code='WBCS';
	
create table #hashStgDimisclaim as
	select	
	source_app_code
	,source_data_set
	,dml_ind
	,record_created_date,
	record_updated_date,
	record_created_by,
	record_updated_by,
	record_eff_from_date
	,record_eff_to_date,
	active_record_ind
	,claim_uuid
	,business_key
	,claim_event_no
	,claim_no
	,claim_event_occurance_date
	,claim_event_type_id
	,claim_type_id
	,claim_event_status_id
	,claim_status_id
	,claim_event_closed_date
	,claim_closed_date
	,claim_reported_date
	,claim_description
	,claim_submission_type
	,policy_uuid
	,customer_uuid
	,product_uuid
	,sub_product_uuid
	,sales_agent_uuid
	,sales_agent_code
	,servicing_agent_uuid
	,claim_event_type_code
	,claim_type_code
	,claim_event_status_code
	,claim_status_code
	,policy_id
	,policy_no
	,sha2(coalesce(cast(source_app_code as varchar),cast('null' as varchar))+
	coalesce(cast(source_data_set as varchar),cast('null' as varchar))+
	coalesce(cast(claim_uuid as varchar),cast('null' as varchar))+
	coalesce(cast(business_key as varchar),cast('null' as varchar))+
	coalesce(cast(claim_event_no as varchar),cast('null' as varchar))+
	coalesce(cast(claim_no as varchar),cast('null' as varchar))+
	coalesce(cast(claim_event_occurance_date as varchar),cast('null' as varchar))+
	coalesce(cast(claim_event_type_id as varchar),cast('null' as varchar))+
	coalesce(cast(claim_type_id as varchar),cast('null' as varchar))+
	coalesce(cast(claim_event_status_id as varchar),cast('null' as varchar))+
	coalesce(cast(claim_status_id as varchar),cast('null' as varchar))+
	coalesce(cast(claim_event_closed_date as varchar),cast('null' as varchar))+
	coalesce(cast(claim_closed_date as varchar),cast('null' as varchar))+
	coalesce(cast(claim_reported_date as varchar),cast('null' as varchar))+
	coalesce(cast(claim_description as varchar),cast('null' as varchar))+
	coalesce(cast(claim_submission_type as varchar),cast('null' as varchar))+
	coalesce(cast(policy_uuid as varchar),cast('null' as varchar))+
	coalesce(cast(customer_uuid as varchar),cast('null' as varchar))+
	coalesce(cast(product_uuid as varchar),cast('null' as varchar))+
	coalesce(cast(sub_product_uuid as varchar),cast('null' as varchar))+
	coalesce(cast(sales_agent_uuid as varchar),cast('null' as varchar))+
	coalesce(cast(sales_agent_code as varchar),cast('null' as varchar))+
	coalesce(cast(servicing_agent_uuid as varchar),cast('null' as varchar))+
	coalesce(cast(claim_event_type_code as varchar),cast('null' as varchar))+
	coalesce(cast(claim_type_code as varchar),cast('null' as varchar))+
	coalesce(cast(claim_event_status_code as varchar),cast('null' as varchar))+
	coalesce(cast(claim_status_code as varchar),cast('null' as varchar))+
	coalesce(cast(policy_id as varchar),cast('null' as varchar))+
	coalesce(cast(policy_no as varchar),cast('null' as varchar)),256) as checksum from el_eds_def_stg.tempstgdimisclaim where source_app_code='WBCS';
	
create table #dimclaim_tgt as
Select
dml_ind,
checksum,
business_key,
active_record_ind
from(Select
dml_ind,
checksum,
business_key,
active_record_ind,
row_number() over(partition by business_key order by record_eff_from_date desc, record_eff_to_date desc) as rnk
from el_eds_def.dimclaim where source_app_code='WBCS')
where rnk=1;

create table #stgdimisclaim AS
select * from (select a.* , case when a.dml_ind='D' and b.dml_ind<>'D' then 1 when a.dml_ind in('I','U') AND b.dml_ind='D' THEN 1 when a.checksum <> coalesce(b.checksum,'1') and coalesce(b.active_record_ind,'Y')='Y' then 1 else 0 end as changed_rec_check from #hashStgDimisclaim a left outer join #dimclaim_tgt b on a.business_key = b.business_key ) where changed_rec_check =1;



DELETE FROM el_eds_def_stg.stgdimclaim WHERE source_app_code='WBCS';

INSERT INTO el_eds_def_stg.stgdimclaim (
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
			claim_uuid,
			business_key
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
			,policy_no)
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
			checksum,
			claim_uuid,
			business_key
			, NULL as claim_event_id
			, NULL as claim_id
			, claim_event_no as claim_event_no
			, claim_no as claim_no
			, claim_event_occurance_date as claim_event_occurance_date
			, NULL as claim_event_location
			, NULL as claim_event_reporting_24hrs
			, claim_event_type_id as claim_event_type_id
			, claim_type_id as claim_type_id
			, claim_event_status_id as claim_event_status_id
			, claim_status_id as claim_status_id
			, claim_event_closed_date as claim_event_closed_date
			, claim_closed_date as claim_closed_date
			, NULL as country_of_accident
			, NULL as notification_date
			, claim_reported_date as claim_reported_date
			,-1 as claim_level_id
			,-1 as claim_source_id
			,-1 as claim_damage_type_id
			,-1 as claim_injury_type_id
			, NULL as towing_required
			,'-1' as tp_clinic_uuid
			, NULL as tp_driver_nric
			,'-1' as tp_insurer_uuid
			,'-1' as tp_lawyer_uuid
			,'-1' as tp_surveyor_uuid
			, NULL as tp_vehicle_no
			,'-1' as tp_workshop_uuid
			, NULL as workshop_repairer
			, NULL as no_adl
			, NULL as claim_insured_liability
			, NULL as claim_insured_liability_desc
			, claim_description as claim_desc
			, NULL as name_of_preferred_workshop
			, NULL as orange_force
			, NULL as our_lawyer
			, claim_submission_type as claim_submission_type
			, NULL as icm_no
			, NULL as tca
			, NULL as wbcs_flag
			, NULL as ecode
			, NULL as ecode_reason
			, NULL as od_excess
			, NULL as tp_excess
			, NULL as additional_excess
			, NULL as windscreen_excess
			, NULL as unnamed_driver_excess
			,'-1' as claim_officer_staff_uuid
			,'-1' as claim_creator_staff_uuid
			, NULL as wbcs_claim_no
			, NULL as claim_status_loss
			, NULL as claim_status_recovery
			, NULL as claim_status_salvage
			, NULL as cause_of_loss
			, policy_uuid as policy_uuid
			, customer_uuid as customer_uuid
			, product_uuid as product_uuid
			, sub_product_uuid as sub_product_uuid
			, sales_agent_uuid as sales_agent_uuid
			, sales_agent_code as sales_agent_code
			, servicing_agent_uuid as servicing_agent_uuid
			, NULL as claim_occurrance_time_hhmm
			,-1 as claimant_type_id
			, NULL as gst_registered
			, NULL as gst_verified
			, claim_event_type_code as claim_event_type_code
			, claim_type_code as claim_type_code
			, claim_event_status_code as claim_event_status_code
			, claim_status_code as claim_status_code
			, policy_id as policy_id
			, policy_no as policy_no
	FROM #stgdimisclaim;
	

-----AUDIT-----

create table #min_record_eff_from_date as
select
  min(record_eff_from_date) AS min_record_eff_from_date,
  'WBCS-ISIS'::VARCHAR(50) AS source_data_set, 
  'WBCS'::VARCHAR(50) AS source_app_code
from	
(
select max(record_eff_from_date) as record_eff_from_date from tl_wbcs_def.tb_claims_hist where active_record_ind = 'Y'
Union   
select max(record_eff_from_date) as record_eff_from_date from tl_wbcs_def.tb_isclaims_hist where active_record_ind = 'Y'
Union
select max(record_eff_from_date) as record_eff_from_date from tl_wbcs_def.tb_isclaimsdetails_hist where active_record_ind = 'Y'
Union
select max(record_eff_from_date) as record_eff_from_date from tl_wbcs_def.tb_pmiadv_main_hist where active_record_ind = 'Y'
Union
select max(record_eff_from_date) as record_eff_from_date from tl_wbcs_def.tb_DiagnosisCode_hist where active_record_ind = 'Y'
Union
select max(record_eff_from_date) as record_eff_from_date from tl_wbcs_def.tb_issubclaims_hist where active_record_ind = 'Y'
);


DELETE FROM el_eds_def_stg.ctrl_audit_stg where tgt_table_name= 'dimclaim' and tgt_source_app_code='WBCS';	


Insert Into el_eds_def_stg.ctrl_audit_stg
select
  source_data_set as tgt_source_data_set,
  source_app_code tgt_source_app_code,
  'el_eds_def' as tgt_schema,
  'dimclaim' as tgt_table_name,
  min_record_eff_from_date as src_record_eff_from_date,
  Null as tgt_record_eff_from_date,
  getdate() as data_pipeline_run_date,
  getdate() as record_created_date,
  getdate() as record_updated_date
from #min_record_eff_from_date;

----------------------------------------------------
create table #src_count as(
 SELECT
        COUNT(1) AS source_count, 
        'tl_wbcs_def' AS src_schema, 
        'tb_claims_hist'::VARCHAR(1000) AS src_table, 
        'WBCS-ISIS'::VARCHAR(50) AS entity
from(
   SELECT DISTINCT claimno,active_record_ind 
FROM  #tb_claims_hist WHERE upper(ClaimType) IN ('IS','ISX')
)
where active_record_ind='Y');

DELETE FROM el_eds_def_stg.dq_audit_stg WHERE entity in (select entity from #src_count) and tgt_table ='dimclaim';

insert into
  el_eds_def_stg.dq_audit_stg(
    select
      'eds' as app_name,
      src_schema,
      src_table,
      'el_eds_def' as tgt_schema,
      'dimclaim' as tgt_table,
      entity,
      '*' as instance,
      'count' as check_type,
      source_count
	from #src_count);
	

	
DROP TABLE IF EXISTS  #v_rundate;
DROP TABLE IF EXISTS  #tb_claims_hist;
DROP TABLE IF EXISTS  #tb_isclaims_hist;
DROP TABLE IF EXISTS  #tb_isclaimsdetails_hist;
DROP TABLE IF EXISTS  #tb_pmiadv_main_hist;
DROP TABLE IF EXISTS  #tb_DiagnosisCode_hist;
DROP TABLE IF EXISTS  #tb_issubclaims_hist;
DROP TABLE IF EXISTS  #DimClaimEventTypeMapping;
DROP TABLE IF EXISTS  #DimClaimTypeMapping;
DROP TABLE IF EXISTS  #DimClaimEventStatusMapping;
DROP TABLE IF EXISTS  #DimClaimStatusMapping;
DROP TABLE IF EXISTS  #DimDiagnosis;
DROP TABLE IF EXISTS  #DimClaim;
DROP TABLE IF EXISTS  #DimPolicy;
DROP TABLE IF EXISTS  #DimAgent;
DROP TABLE IF EXISTS  #ISSubClaims;
DROP TABLE IF EXISTS  #hashStgDimisclaim;
DROP TABLE IF EXISTS  #stgdimisclaim;
DROP TABLE IF EXISTS  #tempstgdimisclaim;
DROP TABLE IF EXISTS  #temp;
DROP TABLE IF EXISTS  #ISX;
DROP TABLE IF EXISTS  #PKPrimary;
DROP TABLE IF EXISTS  #PKPrimary_stg;
END;


