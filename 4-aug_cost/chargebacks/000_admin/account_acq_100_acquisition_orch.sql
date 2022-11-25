------------------------------------------------------------------------------
-- 100_acquisition
--
!print 100_acquisition

-- acquisition scripts that need to run against each source account.
!print 100_acquisition Step 1

!source 100_acquisition/automatic_clustering_history_acq.sql
!source 100_acquisition/data_transfer_history_acq.sql
!source 100_acquisition/database_storage_usage_history_acq.sql
!source 100_acquisition/materialized_view_refresh_history_acq.sql
!source 100_acquisition/metering_daily_history_acq.sql
!source 100_acquisition/pipe_usage_history_acq.sql
!source 100_acquisition/query_history_acq.sql
!source 100_acquisition/replication_usage_history_acq.sql
!source 100_acquisition/search_optimization_history_acq.sql
!source 100_acquisition/stage_storage_usage_history_acq.sql
!source 100_acquisition/warehouse_load_history_acq.sql
!source 100_acquisition/warehouse_metering_history_acq.sql

!source 100_acquisition/warehouse_metering_history_reader_acq.sql
!source 100_acquisition/query_history_reader_acq.sql

list @&{l_common_db}.&{l_common_schema}.account_usage_stg;

!print SUCCESS!
