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
    &{l_raw_db}.&{l_raw_schema}.metering_daily_history
with l_stg as
(
    select
        -- generate hash key and hash diff to streamline processing
        sha1_binary( concat( s.organization_name
                            ,'|', s.region_name
                            ,'|', s.account_name
                            ,'|', s.service_type
                            ,'|', to_char( s.usage_date, 'yyyy-mmm-dd hh24:mi:ss.FF3 TZHTZM'  )
                           )
                    )               as dw_metering_daily_history_shk
        ,s.*
    from
        &{l_raw_db}.&{l_raw_schema}.metering_daily_history_stg s
)
,l_deduped as
(
    select
        *
    from
        (
        select
             -- identify dupes and only keep copy 1
             row_number() over( partition by dw_metering_daily_history_shk order by 1 ) as seq_no
            ,s.*
        from
            l_stg s
        )
    where
        seq_no = 1 -- keep only unique rows
)
select
     s.dw_metering_daily_history_shk
    ,s.organization_name
    ,s.account_name
    ,s.region_name
    ,s.service_type
    ,s.usage_date
    ,s.credits_used_compute
    ,s.credits_used_cloud_services
    ,s.credits_used
    ,s.credits_adjustment_cloud_services
    ,s.credits_billed
    ,s.dw_file_name
    ,s.dw_file_row_no
    ,current_timestamp()    as dw_load_ts
from
    l_deduped s
where
    s.dw_metering_daily_history_shk not in
    (
        select dw_metering_daily_history_shk from &{l_raw_db}.&{l_raw_schema}.metering_daily_history where usage_date >= $l_start_dt and usage_date < $l_end_dt
    )
order by
    usage_date  -- physically sort rows by a logical partitioning date
;
