--------------------------------------------------------------------
--  Purpose:
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
    &{l_raw_db}.&{l_raw_schema}.materialized_view_refresh_history
with l_stg as
(
    select
        -- generate hash key to streamline processing
        sha1_binary( concat( s.organization_name
                            ,'|', s.region_name
                            ,'|', s.account_name
                            ,'|', ifnull( s.database_name, '~' )
                            ,'|', ifnull( s.schema_name, '~' )
                            ,'|', ifnull( s.table_name, '~' )
                            ,'|', to_char( s.start_time, 'yyyy-mmm-dd hh24:mi:ss.FF3 TZHTZM'  )
                          )
                    )               as dw_materialized_view_refresh_history_shk
        ,s.*
    from
        &{l_raw_db}.&{l_raw_schema}.materialized_view_refresh_history_stg s
)
,l_deduped as
(
    select
        *
    from
        (
        select
             -- identify dupes and only keep copy 1
             row_number() over( partition by dw_materialized_view_refresh_history_shk order by 1 ) as seq_no
            ,s.*
        from
            l_stg s
        )
    where
        seq_no = 1 -- keep only unique rows
)
select
     s.dw_materialized_view_refresh_history_shk
    ,s.organization_name
    ,s.account_name
    ,s.region_name
    ,s.start_time
    ,s.end_time
    ,s.credits_used
    ,s.table_id
    ,s.table_name
    ,s.schema_id
    ,s.schema_name
    ,s.database_id
    ,s.database_name
    ,s.dw_file_name
    ,s.dw_file_row_no
    ,current_timestamp()    as dw_load_ts
from
    l_deduped s
where
    s.dw_materialized_view_refresh_history_shk not in
    (
        select dw_materialized_view_refresh_history_shk from &{l_raw_db}.&{l_raw_schema}.materialized_view_refresh_history where start_time >= $l_start_dt and start_time < $l_end_dt
    )
order by
    start_time  -- physically sort rows by a logical partitioning date
;
