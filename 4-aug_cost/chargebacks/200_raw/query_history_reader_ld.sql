--------------------------------------------------------------------
--  Purpose: insert new and modified values into history table
--      This shows and insert-only pattern where every version of a
--      received row is kept.
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------

--------------------------------------------------------------------
-- pull staged delta date range
--
set (l_start_dt, l_end_dt ) = (select start_dt, dateadd( day, 1, end_dt ) from table( &{l_common_db}.&{l_common_schema}.dw_delta_date_range_f( 'all' ) ));

--------------------------------------------------------------------
-- load delta
--
insert into
    &{l_raw_db}.&{l_raw_schema}.query_history_reader
with l_stg as
(
    select
        -- generate hash key and hash diff to streamline processing
        sha1_binary( concat( s.organization_name
                            ,'|', s.region_name
                            ,'|', s.account_name
                            ,'|', s.reader_account_name
                            ,'|', s.query_id
                           )
                    )               as dw_query_history_shk
        ,s.*
    from
        &{l_raw_db}.&{l_raw_schema}.query_history_reader_stg s
)
,l_deduped as
(
    select
        *
    from
        (
        select
             -- identify dupes and only keep copy 1
             row_number() over( partition by dw_query_history_shk order by 1 ) as seq_no
            ,s.*
        from
            l_stg s
        )
    where
        seq_no = 1 -- keep only unique rows
)
select
     s.dw_query_history_shk
    ,s.organization_name
    ,s.account_name
    ,s.region_name
    ,s.reader_account_name
    ,s.query_id
    ,s.query_text
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
    ,s.dw_file_name
    ,s.dw_file_row_no
    ,current_timestamp()    as dw_load_ts
from
    l_deduped s
where
    s.dw_query_history_shk not in
    (
        select dw_query_history_shk from &{l_raw_db}.&{l_raw_schema}.query_history_reader where start_time >= $l_start_dt and start_time < $l_end_dt
    )
order by
    start_time  -- physically sort rows by a logical partitioning date
;
