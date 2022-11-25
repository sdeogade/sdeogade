--------------------------------------------------------------------
--  Purpose: data acquisition
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------

--------------------------------------------------------------------
-- !!! Note: this first step is an example of what would need to be
--           executed against the target account to get the last
--           control date high water mark.  When implemented this
--           would be executed with a wrapper script like python and
--           then the date could be used to execute the copy into step
--           against the source account.
--
-- pull last event date from the table and back up n hours for late
-- arriving data.
--
/*
set l_last_control_dt =
(
    select
        ifnull( dateadd( hour, -4, max( start_time ) ), dateadd( month, -13, current_timestamp() ) ) as last_control_dt
    from
        &{l_raw_db}.&{l_raw_schema}.query_history_reader
    where
            organization_name = upper( '&{l_org_name}' )
        and account_name      = upper( '&{l_account_name}' )
);
*/

--------------------------------------------------------------------
-- !!! Note: code to use when demonstrating across multiple accounts.
--           uses a fixed delta window.

set l_last_control_dt =
(
    select dateadd( hour, -&{l_delta_hours}, current_timestamp() ) as last_control_dt
);


copy into
   @&{l_common_db}.&{l_common_schema}.account_usage_stg/reader_query_history/&{l_account_name}
from
(
    select
         upper( '&{l_org_name}' )           as organization_name
        ,current_account()                  as account_name
        ,current_region()                   as region_name
        ,reader_account_name                AS reader_account_name
        ,s.query_id
        ,regexp_replace(replace(s.query_text, '"'),'[^[:print:]]','')    AS query_text
        ,s.database_id
        ,s.database_name
        ,s.schema_id
        ,s.schema_name
        ,s.query_type
        ,s.session_id
        ,s.user_name
        ,s.role_name
        ,s.warehouse_id
        ,s.warehouse_name
        ,s.warehouse_size
        ,s.warehouse_type
        ,s.cluster_number
        ,s.query_tag
        ,s.execution_status
        ,s.error_code
        ,s.error_message
        ,s.start_time
        ,s.end_time
        ,s.total_elapsed_time
        ,s.bytes_scanned
        ,s.rows_produced
        ,s.compilation_time
        ,s.execution_time
        ,s.queued_provisioning_time
        ,s.queued_repair_time
        ,s.queued_overload_time
        ,s.transaction_blocked_time
        ,s.outbound_data_transfer_cloud
        ,s.outbound_data_transfer_region
        ,s.outbound_data_transfer_bytes
        ,s.inbound_data_transfer_cloud
        ,s.inbound_data_transfer_region
        ,s.inbound_data_transfer_bytes
        ,s.list_external_files_time
        ,s.credits_used_cloud_services
        ,s.reader_account_deleted_on
    from
        snowflake.reader_account_usage.query_history s
    where
        s.start_time >= to_timestamp( $l_last_control_dt )
)
file_format      = ( type=csv field_optionally_enclosed_by = '"' )
include_query_id = true
overwrite        = false
single           = false
;

list @&{l_common_db}.&{l_common_schema}.account_usage_stg;
