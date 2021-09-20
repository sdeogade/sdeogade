------------------------------------------------------------------------------
-- 420_fact_aggregate
--
!print 420_fact_aggregate

-- load resource and account fact tables with actuals
!print 420_fact_aggregate Step 1

!source 420_fact_aggregate/cb_resource_mf_ld.sql
!source 420_fact_aggregate/cb_account_mf_ld.sql
!source 420_fact_aggregate/cb_resource_df_ld.sql
!source 420_fact_aggregate/cb_account_df_ld.sql
!source 420_fact_aggregate/cb_be_budget_mf_ld.sql
-- update account fact with forecast
!print 420_fact_aggregate Step 2

!source 420_fact_aggregate/cb_account_mf_fcst_ld.sql

!print SUCCESS!
