--------------------------------------------------------------------
--  Purpose: load into stage table
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------

truncate table &{l_raw_db}.&{l_raw_schema}.query_history_stg;

copy into
    &{l_raw_db}.&{l_raw_schema}.query_history_stg
from
    (
    select
         s.$1                                            -- organization_name
        ,s.$2                                            -- account_name
        ,s.$3                                            -- region_name
        ,s.$4                                            -- query_id
        ,s.$5                                            -- query_text
        ,s.$6                                            -- database_id
        ,s.$7                                            -- database_name
        ,s.$8                                            -- schema_id
        ,s.$9                                            -- schema_name
        ,s.$10                                           -- query_type
        ,s.$11                                           -- session_id
        ,s.$12                                           -- user_name
        ,s.$13                                           -- role_name
        ,s.$14                                           -- warehouse_id
        ,s.$15                                           -- warehouse_name
        ,s.$16                                           -- warehouse_size
        ,s.$17                                           -- warehouse_type
        ,s.$18                                           -- cluster_number
        ,s.$19                                           -- query_tag
        ,s.$20                                           -- execution_status
        ,s.$21                                           -- error_code
        ,s.$22                                           -- error_message
        ,s.$23                                           -- start_time
        ,s.$24                                           -- end_time
        ,s.$25                                           -- total_elapsed_time
        ,s.$26                                           -- bytes_scanned
        ,s.$27                                           -- percentage_scanned_from_cache
        ,s.$28                                           -- bytes_written
        ,s.$29                                           -- bytes_written_to_result
        ,s.$30                                           -- bytes_read_from_result
        ,s.$31                                           -- rows_produced
        ,s.$32                                           -- rows_inserted
        ,s.$33                                           -- rows_updated
        ,s.$34                                           -- rows_deleted
        ,s.$35                                           -- rows_unloaded
        ,s.$36                                           -- bytes_deleted
        ,s.$37                                           -- partitions_scanned
        ,s.$38                                           -- partitions_total
        ,s.$39                                           -- bytes_spilled_to_local_storage
        ,s.$40                                           -- bytes_spilled_to_remote_storage
        ,s.$41                                           -- bytes_sent_over_the_network
        ,s.$42                                           -- compilation_time
        ,s.$43                                           -- execution_time
        ,s.$44                                           -- queued_provisioning_time
        ,s.$45                                           -- queued_repair_time
        ,s.$46                                           -- queued_overload_time
        ,s.$47                                           -- transaction_blocked_time
        ,s.$48                                           -- outbound_data_transfer_cloud
        ,s.$49                                           -- outbound_data_transfer_region
        ,s.$50                                           -- outbound_data_transfer_bytes
        ,s.$51                                           -- inbound_data_transfer_cloud
        ,s.$52                                           -- inbound_data_transfer_region
        ,s.$53                                           -- inbound_data_transfer_bytes
        ,s.$54                                           -- list_external_files_time
        ,s.$55                                           -- credits_used_cloud_services
        ,s.$56                                           -- release_version
        ,s.$57                                           -- external_function_total_invocations
        ,s.$58                                           -- external_function_total_sent_rows
        ,s.$59                                           -- external_function_total_received_rows
        ,s.$60                                           -- external_function_total_sent_bytes
        ,s.$61                                           -- external_function_total_received_bytes
        ,s.$62                                           -- query_load_percent
        ,to_boolean( s.$63 )                             -- is_client_generated_statement
        ,metadata$filename                               -- dw_file_name
        ,metadata$file_row_number                        -- dw_file_row_no
        ,current_timestamp()                             -- dw_load_ts
    from
        @&{l_common_db}.&{l_common_schema}.account_usage_stg/query_history s
    )
file_format = ( type=csv field_optionally_enclosed_by = '"' )
purge = true
;
