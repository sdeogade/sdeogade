--------------------------------------------------------------------
--  Purpose: create psa tables
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------

--
-- transient staging table with no retention days
--
create transient table if not exists &{l_raw_db}.&{l_raw_schema}.query_history_stg
(
     organization_name                      varchar( 250 )      not null
    ,account_name                           varchar( 250 )      not null
    ,region_name                            varchar( 250 )      not null
    ,query_id                               text                not null
    ,query_text                             text                not null
    ,database_id                            number
    ,database_name                          text
    ,schema_id                              number
    ,schema_name                            text
    ,query_type                             text
    ,session_id                             number
    ,user_name                              text
    ,role_name                              text
    ,warehouse_id                           number
    ,warehouse_name                         text
    ,warehouse_size                         text
    ,warehouse_type                         text
    ,cluster_number                         number
    ,query_tag                              text
    ,execution_status                       text
    ,error_code                             number
    ,error_message                          text
    ,start_time                             timestamp_ltz
    ,end_time                               timestamp_ltz
    ,total_elapsed_time                     number
    ,bytes_scanned                          number
    ,percentage_scanned_from_cache          float
    ,bytes_written                          number
    ,bytes_written_to_result                number
    ,bytes_read_from_result                 number
    ,rows_produced                          number
    ,rows_inserted                          number
    ,rows_updated                           number
    ,rows_deleted                           number
    ,rows_unloaded                          number
    ,bytes_deleted                          number
    ,partitions_scanned                     number
    ,partitions_total                       number
    ,bytes_spilled_to_local_storage         number
    ,bytes_spilled_to_remote_storage        number
    ,bytes_sent_over_the_network            number
    ,compilation_time                       number
    ,execution_time                         number
    ,queued_provisioning_time               number
    ,queued_repair_time                     number
    ,queued_overload_time                   number
    ,transaction_blocked_time               number
    ,outbound_data_transfer_cloud           text
    ,outbound_data_transfer_region          text
    ,outbound_data_transfer_bytes           number
    ,inbound_data_transfer_cloud            text
    ,inbound_data_transfer_region           text
    ,inbound_data_transfer_bytes            number
    ,list_external_files_time               number
    ,credits_used_cloud_services            number
    ,release_version                        text
    ,external_function_total_invocations    number
    ,external_function_total_sent_rows      number
    ,external_function_total_received_rows  number
    ,external_function_total_sent_bytes     number
    ,external_function_total_received_bytes number
    ,query_load_percent                     number
    ,is_client_generated_statement          boolean
    --
    ,dw_file_name                           varchar( 250 )      not null
    ,dw_file_row_no                         number              not null
    ,dw_load_ts                             timestamp_ltz       not null
)
data_retention_time_in_days = 0
;

--
-- permanent history table with retention days
--
create table if not exists &{l_raw_db}.&{l_raw_schema}.query_history
(
     dw_query_history_shk                           binary( 20 )        not null
    --
    ,organization_name                      varchar( 250 )      not null
    ,account_name                           varchar( 250 )      not null
    ,region_name                            varchar( 250 )      not null
    ,query_id                               text                not null
    ,query_text                             text                not null
    ,database_id                            number
    ,database_name                          text
    ,schema_id                              number
    ,schema_name                            text
    ,query_type                             text
    ,session_id                             number
    ,user_name                              text
    ,role_name                              text
    ,warehouse_id                           number
    ,warehouse_name                         text
    ,warehouse_size                         text
    ,warehouse_type                         text
    ,cluster_number                         number
    ,query_tag                              text
    ,execution_status                       text
    ,error_code                             number
    ,error_message                          text
    ,start_time                             timestamp_ltz
    ,end_time                               timestamp_ltz
    ,total_elapsed_time                     number
    ,bytes_scanned                          number
    ,percentage_scanned_from_cache          float
    ,bytes_written                          number
    ,bytes_written_to_result                number
    ,bytes_read_from_result                 number
    ,rows_produced                          number
    ,rows_inserted                          number
    ,rows_updated                           number
    ,rows_deleted                           number
    ,rows_unloaded                          number
    ,bytes_deleted                          number
    ,partitions_scanned                     number
    ,partitions_total                       number
    ,bytes_spilled_to_local_storage         number
    ,bytes_spilled_to_remote_storage        number
    ,bytes_sent_over_the_network            number
    ,compilation_time                       number
    ,execution_time                         number
    ,queued_provisioning_time               number
    ,queued_repair_time                     number
    ,queued_overload_time                   number
    ,transaction_blocked_time               number
    ,outbound_data_transfer_cloud           text
    ,outbound_data_transfer_region          text
    ,outbound_data_transfer_bytes           number
    ,inbound_data_transfer_cloud            text
    ,inbound_data_transfer_region           text
    ,inbound_data_transfer_bytes            number
    ,list_external_files_time               number
    ,credits_used_cloud_services            number
    ,release_version                        text
    ,external_function_total_invocations    number
    ,external_function_total_sent_rows      number
    ,external_function_total_received_rows  number
    ,external_function_total_sent_bytes     number
    ,external_function_total_received_bytes number
    ,query_load_percent                     number
    ,is_client_generated_statement          boolean
    --
    ,dw_file_name                           varchar( 250 )      not null
    ,dw_file_row_no                         number              not null
    ,dw_load_ts                             timestamp_ltz       not null
)
data_retention_time_in_days = 1
;
