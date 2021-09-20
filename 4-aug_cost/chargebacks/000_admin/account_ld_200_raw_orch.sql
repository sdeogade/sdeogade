------------------------------------------------------------------------------
-- 200_raw
--
!print 200_raw

-- reference data needed by downstream processes for mapping wareho--use and database resources to business entities
-- also cost numbers used to convert credits to cost of consumption
!print 200_raw Step 1

!source 200_raw/be_resource_mapping_lkp_ld.sql
!source 200_raw/cb_service_type_lkp_ld.sql
!source 200_raw/sf_compute_cost_lkp_ld.sql
!source 200_raw/sf_data_xfer_cost_lkp_ld.sql
!source 200_raw/sf_storage_cost_lkp_ld.sql
!source 200_raw/be_budget_cost_lkp_ld.sql

-- load files from acquisition sources into stage tables
!print 200_raw Step 2

!source 200_raw/automatic_clustering_history_stg_ld.sql
!source 200_raw/data_transfer_history_stg_ld.sql
!source 200_raw/database_storage_usage_history_stg_ld.sql
!source 200_raw/materialized_view_refresh_history_stg_ld.sql
!source 200_raw/metering_daily_history_stg_ld.sql
!source 200_raw/pipe_usage_history_stg_ld.sql
!source 200_raw/search_optimization_history_stg_ld.sql
!source 200_raw/query_history_stg_ld.sql
!source 200_raw/replication_usage_history_stg_ld.sql
!source 200_raw/stage_storage_usage_history_stg_ld.sql
!source 200_raw/warehouse_load_history_stg_ld.sql
!source 200_raw/warehouse_metering_history_stg_ld.sql

-- reader accounts
!source 200_raw/warehouse_metering_history_reader_stg_ld.sql
!source 200_raw/query_history_reader_stg_ld.sql

-- load distinct dates represented within staged data
!print 200_raw Step 3

!source 200_raw/dw_delta_date_ld.sql

-- load staged data into target tables, using delta dates to optimize partition pruning
!print 200_raw Step 4

!source 200_raw/automatic_clustering_history_ld.sql
!source 200_raw/data_transfer_history_ld.sql
!source 200_raw/database_storage_usage_history_ld.sql
!source 200_raw/materialized_view_refresh_history_ld.sql
!source 200_raw/metering_daily_history_ld.sql
!source 200_raw/pipe_usage_history_ld.sql
!source 200_raw/query_history_ld.sql
!source 200_raw/replication_usage_history_ld.sql
!source 200_raw/search_optimization_history_ld.sql
!source 200_raw/stage_storage_usage_history_ld.sql
!source 200_raw/warehouse_load_history_ld.sql
!source 200_raw/warehouse_metering_history_ld.sql

-- Reader accounts
!source 200_raw/warehouse_metering_history_reader_ld.sql
!source 200_raw/query_history_reader_ld.sql

!print SUCCESS!
