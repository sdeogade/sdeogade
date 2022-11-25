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
    &{l_raw_db}.&{l_raw_schema}.warehouse_metering_history_reader
with l_stg as
(
    select
        -- generate hash key and hash diff to streamline processing
        sha1_binary( concat( s.organization_name
                            ,'|', s.region_name
                            ,'|', s.account_name
                            ,'|', s.reader_account_name
                            ,'|', s.warehouse_name
                            ,'|', to_char( s.start_time, 'yyyy-mmm-dd hh24:mi:ss.FF3 TZHTZM'  )
                           )
                    )               as dw_warehouse_metering_history_shk
        ,s.*
    from
        &{l_raw_db}.&{l_raw_schema}.warehouse_metering_history_reader_stg s
)
,l_deduped as
(
    select
        *
    from
        (
        select
             -- identify dupes and only keep copy 1
             row_number() over( partition by dw_warehouse_metering_history_shk order by 1 ) as seq_no
            ,s.*
        from
            l_stg s
        )
    where
        seq_no = 1 -- keep only unique rows
)
select
     s.dw_warehouse_metering_history_shk
    ,s.organization_name
    ,s.account_name
    ,s.region_name
    ,s.reader_account_name
    ,s.start_time
    ,s.end_time
    ,s.warehouse_id
    ,s.warehouse_name
    ,s.credits_used
    ,s.credits_used_compute
    ,s.credits_used_cloud_services
    ,s.reader_account_deleted_on
    ,s.dw_file_name
    ,s.dw_file_row_no
    ,current_timestamp()    as dw_load_ts
from
    l_deduped s
where
    s.dw_warehouse_metering_history_shk not in
    (
        select dw_warehouse_metering_history_shk from &{l_raw_db}.&{l_raw_schema}.warehouse_metering_history_reader where start_time >= $l_start_dt and start_time < $l_end_dt
    )
order by
    start_time  -- physically sort rows by a logical partitioning date
;
