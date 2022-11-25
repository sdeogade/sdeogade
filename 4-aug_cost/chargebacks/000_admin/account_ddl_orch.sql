------------------------------------------------------------------------------
-- 000_admin
--
!print 000_admin

!source 000_admin/wrk_environment.sql

------------------------------------------------------------------------------
-- 900_common
--
!print 900_common

!source 900_common/account_usage_stg.sql
!source 900_common/date_sid_f.sql
!source 900_common/dw_delta_date_tbl.sql
!source 900_common/dw_delta_date_ld_sp.sql
!source 900_common/dw_delta_date_range_f.sql
!source 900_common/fcst_linear_trend_sp.sql
!source 900_common/fcst_seasonal_trend_sp.sql

------------------------------------------------------------------------------
-- 100_acquisition
--
!print 100_acquisition

------------------------------------------------------------------------------
-- 200_raw
--
!print 200_raw

!source 200_raw/automatic_clustering_history_tbl.sql
!source 200_raw/be_resource_mapping_lkp_tbl.sql
!source 200_raw/cb_service_type_lkp_tbl.sql
!source 200_raw/data_transfer_history_tbl.sql
!source 200_raw/database_storage_usage_history_tbl.sql
!source 200_raw/materialized_view_refresh_history_tbl.sql
!source 200_raw/metering_daily_history_tbl.sql
!source 200_raw/pipe_usage_history_tbl.sql
!source 200_raw/query_history_tbl.sql
!source 200_raw/replication_usage_history_tbl.sql
!source 200_raw/search_optimization_history_tbl.sql
!source 200_raw/sf_compute_cost_lkp_tbl.sql
!source 200_raw/sf_data_xfer_cost_lkp_tbl.sql
!source 200_raw/sf_storage_cost_lkp_tbl.sql
!source 200_raw/stage_storage_usage_history_tbl.sql
!source 200_raw/warehouse_load_history_tbl.sql
!source 200_raw/warehouse_metering_history_tbl.sql

-- For reader accounts

!source 200_raw/be_budget_cost_lkp_tbl.sql
!source 200_raw/warehouse_metering_history_reader_tbl.sql
!source 200_raw/query_history_reader_tbl.sql

------------------------------------------------------------------------------
-- 300_integration
--
!print 300_integration

!source 300_integration/cb_account_h_tbl.sql
!source 300_integration/cb_be_h_tbl.sql
!source 300_integration/cb_account_resource_l_tbl.sql
!source 300_integration/cb_resource_h_tbl.sql

------------------------------------------------------------------------------
-- 310_derivation
--
!print 310_derivation

!source 310_derivation/be_resource_mapping_s_tbl.sql
!source 310_derivation/cb_account_consumption_fcst_ms_tbl.sql
!source 310_derivation/cb_account_consumption_ds_tbl.sql
!source 310_derivation/cb_resource_consumption_ds_tbl.sql
!source 310_derivation/cb_account_consumption_ms_tbl.sql
!source 310_derivation/cb_resource_consumption_ms_tbl.sql
!source 310_derivation/cb_job_credits_s_tbl.sql
!source 310_derivation/sf_compute_cost_s_tbl.sql
!source 310_derivation/be_budget_cost_s_tbl.sql
!source 310_derivation/sf_data_xfer_cost_s_tbl.sql
!source 310_derivation/sf_storage_cost_s_tbl.sql

------------------------------------------------------------------------------
-- 320_mdm
--
!print 320_mdm

------------------------------------------------------------------------------
-- 400_dimension
--
!print 400_dimension

!source 400_dimension/cb_account_dm_tbl.sql
!source 400_dimension/cb_resource_dm_tbl.sql
!source 400_dimension/cb_be_dm_tbl.sql
!source 400_dimension/cb_service_type_dm_tbl.sql
!source 400_dimension/date_dm_tbl.sql

------------------------------------------------------------------------------
-- 410_fact_atomic
--
!print 410_fact_atomic

------------------------------------------------------------------------------
-- 420_fact_aggregate
--
!print 420_fact_aggregate

!source 420_fact_aggregate/cb_account_df_tbl.sql
!source 420_fact_aggregate/cb_resource_df_tbl.sql
!source 420_fact_aggregate/cb_account_mf_tbl.sql
!source 420_fact_aggregate/cb_be_budget_mf_tbl.sql 
!source 420_fact_aggregate/cb_resource_mf_tbl.sql

------------------------------------------------------------------------------
-- 500_outbound
--
!print 500_outbound

------------------------------------------------------------------------------
-- 900_common
--
!print 900_common

!source 900_common/sf_compute_cost_f.sql
!source 900_common/sf_data_xfer_cost_f.sql
!source 900_common/sf_storage_cost_f.sql

!print SUCCESS!
