--------------------------------------------------------------------
--  Purpose: load into stage table
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------

truncate table &{l_raw_db}.&{l_raw_schema}.query_history_reader_stg;

copy into
    &{l_raw_db}.&{l_raw_schema}.query_history_reader_stg
from
    (
    select
         s.$1                                            -- organization_name
        ,s.$2                                            -- account_name
        ,s.$3                                            -- region_name
        ,s.$4                                            -- reader_account_name
        ,s.$5                                            -- query_id
        ,s.$6                                            -- query_text
        ,s.$7                                            -- database_id
        ,s.$8                                            -- database_name
        ,s.$9                                            -- schema_id
        ,s.$10                                           -- schema_name
        ,s.$11                                           -- query_type
        ,s.$12                                           -- session_id
        ,s.$13                                           -- user_name
        ,s.$14                                           -- role_name
        ,s.$15                                           -- warehouse_id
        ,s.$16                                           -- warehouse_name
        ,s.$17                                           -- warehouse_size
        ,s.$18                                           -- warehouse_type
        ,s.$19                                           -- cluster_number
        ,s.$20                                           -- query_tag
        ,s.$21                                           -- execution_status
        ,s.$22                                           -- error_code
        ,s.$23                                           -- error_message
        ,s.$24                                           -- start_time
        ,s.$25                                           -- end_time
        ,s.$26                                           -- total_elapsed_time
        ,s.$27                                           -- bytes_scanned
        ,s.$28                                           -- rows_produced
        ,s.$29                                           -- compilation_time
        ,s.$30                                           -- execution_time
        ,s.$31                                           -- queued_provisioning_time
        ,s.$32                                           -- queued_repair_time
        ,s.$33                                           -- queued_overload_time
        ,s.$34                                           -- transaction_blocked_time
        ,s.$35                                           -- outbound_data_transfer_cloud
        ,s.$36                                           -- outbound_data_transfer_region
        ,s.$37                                           -- outbound_data_transfer_bytes
        ,s.$38                                           -- inbound_data_transfer_cloud
        ,s.$39                                           -- inbound_data_transfer_region
        ,s.$40                                           -- inbound_data_transfer_bytes
        ,s.$41                                           -- list_external_files_time
        ,s.$42                                           -- credits_used_cloud_services
        ,s.$43                                           -- reader_account_deleted_on
        ,metadata$filename                               -- dw_file_name
        ,metadata$file_row_number                        -- dw_file_row_no
        ,current_timestamp()                             -- dw_load_ts
    from
        @&{l_common_db}.&{l_common_schema}.account_usage_stg/reader_query_history s
    )
file_format = ( type=csv field_optionally_enclosed_by = '"' )
purge = true
;
