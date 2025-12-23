import sys
import os

sys.path.append("/usr/local/airflow/dags/tds/ebgi-group7")

#from variables import *
from airflow.models import Variable

GLUE_ROLE_ARN        = Variable.get("GLUE_ROLE_ARN")
ENDPOINT_URL         = Variable.get("ENDPOINT_URL")
TDS_JOB_FILE_BUCKET_NAME = Variable.get("TDS_JOB_FILE_BUCKET_NAME")
TDS_TARGET_BUCKET_NAME   = Variable.get("TDS_TARGET_BUCKET_NAME")
TARGET_BUCKET_NAME = Variable.get("TARGET_BUCKET_NAME")
WAREHOUSE_PATH_DIR   = f"s3://{TDS_TARGET_BUCKET_NAME}/athena_warehouse/"

SCRIPT_LOCATION_CG = f"s3://{TDS_JOB_FILE_BUCKET_NAME}/tds/scripts/emr_tds_template_file.py"
SCRIPT_LOCATION_TL = f"s3://{TDS_JOB_FILE_BUCKET_NAME}/tds/scripts/emr_tds_template_transform.py"
SCRIPT_LOCATION_FL = f"s3://{TDS_JOB_FILE_BUCKET_NAME}/tds/scripts/emr_tds_template_merge.py"
SCRIPT_LOCATION_DQ = f"s3://{TDS_JOB_FILE_BUCKET_NAME}/tds/scripts/emr_tds_count_comparision.py"
SCRIPT_LOCATION_COMP = f"s3://{TDS_JOB_FILE_BUCKET_NAME}/tds/scripts/emr_tds_compaction.py"

# def get_job_args():
def get_table_config(src_schema, src_table, tgt_schema, tgt_table):
    table_details = {
        "CATALOG_NAME" : "glue_catalog",
        "WAREHOUSE_PATH" : f"{WAREHOUSE_PATH_DIR}",
        "source_schema"  : src_schema,
        "source_table"   : src_table,
        "target_schema"  : tgt_schema,
        "target_table"  : tgt_table,
        "source_bucket_name" : TDS_JOB_FILE_BUCKET_NAME,
        "target_tds_bucket_name" : TDS_TARGET_BUCKET_NAME,
        "rl_target_bucket_name" : TARGET_BUCKET_NAME,        
        "config_csv_file_path" : f"tds/mappings/ebgi/{tgt_table}_mapping.csv",
        "config_jsn_file_path" : f"tds/config/ebgi/{tgt_table}_config.json",
        "multiple_sources" : "N",
        "batch_flag" : "N"
    }

    return table_details

table_list = [
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_bcp_balance_account_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_bcp_balance_account_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_bcp_chq_oper_track_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_bcp_chq_oper_track_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_bcp_bt_return_record_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_bcp_bt_return_record_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_bcp_collection_confirmation_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_bcp_collection_confirmation_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_bcp_bt_arap_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_bcp_bt_arap_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_bcp_payment_bill_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_bcp_payment_bill_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_bcp_payment_bill_rln_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_bcp_payment_bill_rln_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_bcp_soa_agt_rpt_dtl_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_bcp_soa_agt_rpt_dtl_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_bcp_soa_balance_log_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_bcp_soa_balance_log_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_bcp_csoa_detail_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_bcp_csoa_detail_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_bcp_soa_detail_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_bcp_soa_detail_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_bcp_soa_fee_log_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_bcp_soa_fee_log_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_agt_busi_cate_log_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_agt_busi_cate_log_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_agt_sales_auth_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_agt_sales_auth_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_agt_prdt_rate_his_log_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_agt_prdt_rate_his_log_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_agt_prdt_rate_his_log_dtl_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_agt_prdt_rate_his_log_dtl_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_gri_soa_adjust_detail_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_gri_soa_adjust_detail_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_gri_claim_intf_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_gri_claim_intf_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_gri_fac_xol_cont_log_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_gri_fac_xol_cont_log_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_gri_fac_xol_cont_reins_log_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_gri_fac_xol_cont_reins_log_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_gri_fac_xol_cont_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_gri_fac_xol_cont_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_gri_fac_xol_cont_reins_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_gri_fac_xol_cont_reins_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_rpt_soa_rein_detail_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_rpt_soa_rein_detail_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_pty_account_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_pty_account_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_pty_cust_org_log_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_pty_cust_org_log_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_pty_cust_indi_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_pty_cust_indi_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_pty_cust_indi_log_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_pty_cust_indi_log_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_ptyr_log_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_ptyr_log_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_pty_ncdcompany_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_pty_ncdcompany_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_pty_branch_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_pty_branch_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_pty_bank_srv_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_pty_bank_srv_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_pty_ricompany_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_pty_ricompany_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_pty_ribroker_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_pty_ribroker_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_rpt_soa_balance_detail_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_rpt_soa_balance_detail_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_rpt_soa_aging_stg_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_rpt_soa_aging_stg_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_rpt_soa_accrual_comm_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_rpt_soa_accrual_comm_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_rpt_soa_co_fee_aging_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_rpt_soa_co_fee_aging_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_rpt_soa_os_prem_aging_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_rpt_soa_os_prem_aging_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_rpt_soa_claim_aging_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_rpt_soa_claim_aging_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_rpt_soa_aging_detail_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_rpt_soa_aging_detail_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gc",
        "src_table": "tb_t_icm_clm_mt_recovery_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_icm_clm_mt_recovery_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gc",
        "src_table": "tb_t_clm_policy_party_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_clm_policy_party_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_rpt_clm_reg_accident_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_rpt_clm_reg_accident_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_document_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_document_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_ncd_relation_opt_list_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_ncd_relation_opt_list_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_insured_list_log_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_insured_list_log_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_policy_comm_rate_log_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_policy_comm_rate_log_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_gen_policy_fee_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_gen_policy_fee_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_gen_policy_fee_log_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_gen_policy_fee_log_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_icm_fpl_endo_phase_status_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_icm_fpl_endo_phase_status_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_task_notice_list_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_task_notice_list_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_rpt_prem_upr_ri_stg_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_rpt_prem_upr_ri_stg_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_rpt_prem_upr_stg_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_rpt_prem_upr_stg_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_icm_ci_trans_log_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_icm_ci_trans_log_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_rpt_csoa_accrual_comm_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_rpt_csoa_accrual_comm_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_rpt_daily_os_ap_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_rpt_daily_os_ap_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_rpt_daily_receipt_list_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_rpt_daily_receipt_list_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_icm_endo_certificate_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_icm_endo_certificate_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_rpt_agent_conso_balance_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_rpt_agent_conso_balance_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_product_country_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_product_country_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_rpt_soa_rein_total_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_rpt_soa_rein_total_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_rpt_soa_rein_balance_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_rpt_soa_rein_balance_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_agt_guarantee_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_agt_guarantee_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_agt_guarantee_log_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_agt_guarantee_log_hist",
        "resource_size": "medium"
    },
	{
        "src_schema" : "rl_ebgi_gs",
        "src_table" : "tb_t_bcp_offset_de",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_t_bcp_offset_hist",
        "resource_size" : "medium"
	},
 
    {
        "src_schema" : "rl_ebgi_gs",
        "src_table" : "tb_t_bcp_hono_adjust_rela_de",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_t_bcp_hono_adjust_rela_hist",
        "resource_size" : "medium"
	},
	{
        "src_schema" : "rl_ebgi_gc",
        "src_table" : "tb_t_clm_letter_de",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_t_clm_letter_hist",
        "resource_size" : "medium"
	},
    {
        "src_schema": "rl_ebgi_gc",
        "src_table": "tb_t_clm_document_type_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_clm_document_type_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_marsh_acc_handler_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_marsh_acc_handler_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_gri_fee_type_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_gri_fee_type_hist",
        "resource_size": "medium"
    },
    {
        "src_schema": "rl_ebgi_gs",
        "src_table": "tb_t_icm_policy_source_de",
        "tgt_schema": "tl_ebgi_def",
        "tgt_table": "tb_t_icm_policy_source_hist",
        "resource_size": "medium"
    },
    {
        "src_schema" : "rl_ebgi_gs",
        "src_table" : "tb_t_document_addressee_de",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_t_document_addressee_hist",
        "resource_size" : "medium"
    },
    {
        "src_schema" : "rl_ebgi_gs",
        "src_table" : "tb_t_icm_travel_country_de",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_t_icm_travel_country_hist",
        "resource_size" : "medium"
    },
    {
        "src_schema" : "rl_ebgi_gs",
        "src_table" : "tb_t_disc_loading_type_de",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_t_disc_loading_type_hist",
        "resource_size" : "medium"
    },
    {
        "src_schema" : "rl_ebgi_gs",
        "src_table" : "tb_t_icm_pissuance_phase_status_de",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_t_icm_pissuance_phase_status_hist",
        "resource_size" : "medium"
    },
    {
        "src_schema" : "rl_ebgi_gs",
        "src_table" : "tb_t_country_de",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_t_country_hist",
        "resource_size" : "medium"
    },
    {
        "src_schema" : "rl_ebgi_gs",
        "src_table" : "tb_t_icm_pol_ct_acce_content_de",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_t_icm_pol_ct_acce_content_hist",
        "resource_size" : "medium"
    },
    {
        "src_schema" : "rl_ebgi_gs",
        "src_table" : "tb_t_icm_travel_region_de",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_t_icm_travel_region_hist",
        "resource_size" : "medium"
    },
    {
        "src_schema" : "rl_ebgi_gs",
        "src_table" : "tb_t_payment_plan_de",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_t_payment_plan_hist",
        "resource_size" : "medium"
    },
    {
        "src_schema" : "rl_ebgi_gs",
        "src_table" : "tb_t_instal_charge_interval_de",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_t_instal_charge_interval_hist",
        "resource_size" : "medium"
    },
    {
        "src_schema" : "rl_ebgi_gs",
        "src_table" : "tb_t_icm_policy_plan_type_de",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_t_icm_policy_plan_type_hist",
        "resource_size" : "medium"
    },
    {
        "src_schema" : "rl_ebgi_gc",
        "src_table" : "tb_t_clm_close_type_de",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_t_clm_close_type_hist",
        "resource_size" : "medium"
    },
    {
        "src_schema" : "rl_ebgi_gc",
        "src_table" : "tb_t_clm_party_role_de",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_t_clm_party_role_hist",
        "resource_size" : "medium"
    },
    {
        "src_schema" : "rl_ebgi_gc",
        "src_table" : "tb_t_clm_product_line_de",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_t_clm_product_line_hist",
        "resource_size" : "medium"
    },
    {
        "src_schema" : "rl_ebgi_gc",
        "src_table" : "tb_t_clm_product_type_de",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_t_clm_product_type_hist",
        "resource_size" : "medium"
    },
    {
        "src_schema" : "rl_ebgi_gc",
        "src_table" : "tb_t_clm_referral_form_de",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_t_clm_referral_form_hist",
        "resource_size" : "medium"
    },
    {
        "src_schema" : "rl_ebgi_gs",
        "src_table" : "tb_v_ptm_ncdcompany",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_v_ptm_ncdcompany_hist",
        "resource_size" : "medium"
    },
    {
        "src_schema" : "rl_ebgi_gc",
        "src_table" : "tb_v_icm_od_insured_liability",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_v_icm_od_insured_liability_hist",
        "resource_size" : "medium"
    },
    {
        "src_schema" : "rl_ebgi_gc",
        "src_table" : "tb_v_icm_pc_claim_source",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_v_icm_pc_claim_source_hist",
        "resource_size" : "medium"
    },
    {
        "src_schema" : "rl_ebgi_gc",
        "src_table" : "tb_v_icm_pre_repair_option",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_v_icm_pre_repair_option_hist",
        "resource_size" : "medium"
    },
    {
        "src_schema" : "rl_ebgi_gc",
        "src_table" : "tb_v_icm_recovery_type",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_v_icm_recovery_type_hist",
        "resource_size" : "medium"
    },
    {
        "src_schema" : "rl_ebgi_gc",
        "src_table" : "tb_v_icm_special_remark",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_v_icm_special_remark_hist",
        "resource_size" : "medium"
    },
    {
        "src_schema" : "rl_ebgi_gs",
        "src_table" : "tb_t_icm_vehicle_usage_de ",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_t_icm_vehicle_usage_hist",
        "resource_size" : "medium"
    },
    {
        "src_schema" : "rl_ebgi_gs",
        "src_table" : "tb_jbpm_task_de",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_jbpm_task_hist",
        "resource_size" : "medium"
    },
    {
        "src_schema" : "rl_ebgi_gs",
        "src_table" : "tb_t_bcp_cfg_pay_mode_de",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_t_bcp_cfg_pay_mode_hist",
        "resource_size" : "medium"
    },
    {
        "src_schema" : "rl_ebgi_gs",
        "src_table" : "tb_t_money_de",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_t_money_hist",
        "resource_size" : "medium"
    },
    {
       "src_schema" : "rl_ebgi_gc",
       "src_table" : "tb_t_icm_clm_stp_result_de",
       "tgt_schema" : "tl_ebgi_def",
       "tgt_table" : "tb_t_icm_clm_stp_result_hist",
       "resource_size" : "medium"
    },
    {
       "src_schema" : "rl_ebgi_gc",
       "src_table" : "tb_t_icm_clm_claim_status_rsn_de",
       "tgt_schema" : "tl_ebgi_def",
       "tgt_table" : "tb_t_icm_clm_claim_status_rsn_hist",
       "resource_size" : "medium"
    },
    {
        "src_schema" : "rl_ebgi_gc",
        "src_table" : "tb_t_icm_clm_sampling_chk_outcome_de",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_t_icm_clm_sampling_chk_outcome_hist",
        "resource_size" : "medium"
    },
    {
        "src_schema" : "rl_ebgi_gc",
        "src_table" : "tb_t_icm_clm_non_stp_reason_de",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_t_icm_clm_non_stp_reason_hist",
        "resource_size" : "medium"
    },
    {
        "src_schema" : "rl_ebgi_gs",
        "src_table" : "tb_t_bcp_cfg_fee_type_de",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_t_bcp_cfg_fee_type_hist",
        "resource_size" : "small"

    },
    {
       "src_schema" : "rl_ebgi_gs",
       "src_table" : "tb_t_bcp_cfg_giro_failed_reason_de",
       "tgt_schema" : "tl_ebgi_def",
       "tgt_table" :"tb_t_bcp_cfg_giro_failed_reason_hist",
       "resource_size" : "medium"
    },
    {
       "src_schema" : "rl_ebgi_gc",
       "src_table" : "tb_t_clm_payment_status_de",
       "tgt_schema" : "tl_ebgi_def",
       "tgt_table" :"tb_t_clm_payment_status_hist",
       "resource_size" : "medium"
    },
    {
        "src_schema" : "rl_ebgi_gs",
        "src_table" : "tb_t_bcp_unclaim_money_de",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_t_bcp_unclaim_money_hist",
        "resource_size" : "medium"
    },
    {
        "src_schema" : "rl_ebgi_gs",
        "src_table" : "tb_t_bcp_unclaim_money_dtl_de",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_t_bcp_unclaim_money_dtl_hist",
        "resource_size" : "medium"
    },
    {
        "src_schema" : "rl_ebgi_gs",
        "src_table" : "tb_t_bcp_cfg_balance_type_de",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_t_bcp_cfg_balance_type_hist",
        "resource_size" : "medium"
    },
    {
        "src_schema" : "rl_ebgi_gs",
        "src_table" : "tb_t_gen_riskcate_prem_de",
        "tgt_schema" : "tl_ebgi_def",
        "tgt_table" : "tb_t_gen_riskcate_prem_hist",
        "resource_size" : "medium"
    }
    

] 