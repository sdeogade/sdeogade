------------------------------------------------------------------------------
-- 310_derivation
--
!print 310_derivation

-- map source resources to business entities and assign cost numbers to each resource
!print 310_derivation Step 1

!source 310_derivation/be_resource_mapping_s_ld.sql
!source 310_derivation/sf_compute_cost_s_ld.sql
!source 310_derivation/sf_data_xfer_cost_s_ld.sql
!source 310_derivation/sf_storage_cost_s_ld.sql
!source 310_derivation/be_budget_cost_s_ld.sql

-- monthly aggregation of consumption by resource
!print 310_derivation Step 2

!source 310_derivation/cb_job_credits_s_ld.sql

-- monthly aggregation of consumption by resource
!print 310_derivation Step 3

!source 310_derivation/cb_resource_consumption_ms_ld.sql
!source 310_derivation/cb_resource_consumption_ds_ld.sql
-- monthly aggregation of consumption by account
!print 310_derivation Step 4

!source 310_derivation/cb_account_consumption_ms_ld.sql
!source 310_derivation/cb_account_consumption_ds_ld.sql
-- monthly consumption forecast by account
!print 310_derivation Step 5

!source 310_derivation/cb_account_consumption_fcst_ms_ld.sql

!print SUCCESS!
