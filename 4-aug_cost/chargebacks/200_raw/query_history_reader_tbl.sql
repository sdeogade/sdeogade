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
create transient table if not exists &{l_raw_db}.&{l_raw_schema}.query_history_reader_stg
(
     organization_name                      varchar( 250 )      not null
    ,account_name                           varchar( 250 )      not null
    ,region_name                            varchar( 250 )      not NULL
    ,reader_account_name                    varchar( 250 )      NOT null
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
    ,rows_produced                          number
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
    ,reader_account_deleted_on              timestamp_ltz
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
create table if not exists &{l_raw_db}.&{l_raw_schema}.query_history_reader
(
     dw_query_history_shk                   binary( 20 )        not null
    --
    ,organization_name                      varchar( 250 )      not null
    ,account_name                           varchar( 250 )      not null
    ,region_name                            varchar( 250 )      not NULL
    ,reader_account_name                    varchar( 250 )      NOT null
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
    ,rows_produced                          number
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
    ,reader_account_deleted_on              timestamp_ltz
    --
    ,dw_file_name                           varchar( 250 )      not null
    ,dw_file_row_no                         number              not null
    ,dw_load_ts                             timestamp_ltz       not null
)
data_retention_time_in_days = 1
;
