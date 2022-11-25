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
    &{l_il_db}.&{l_il_schema}.cb_account_h
with l_account as
(
    select distinct
         wmh.organization_name
        ,wmh.region_name
        ,wmh.account_name
        ,'~'                AS reader_account_name
    from
        &{l_raw_db}.&{l_raw_schema}.warehouse_metering_history wmh
    where
        -- wmh
            wmh.start_time        >= $l_start_dt
        and wmh.start_time         < $l_end_dt
    UNION
    select distinct
         wmh.organization_name
        ,wmh.region_name
        ,wmh.account_name
        ,wmh.reader_account_name 
    from
        &{l_raw_db}.&{l_raw_schema}.warehouse_metering_history_reader wmh
    where
        -- wmh
            wmh.start_time        >= $l_start_dt
        and wmh.start_time         < $l_end_dt
    union
    select distinct
         dsuh.organization_name
        ,dsuh.region_name
        ,dsuh.account_name
        ,'~'                AS reader_account_name
    from
        &{l_raw_db}.&{l_raw_schema}.database_storage_usage_history dsuh
    where
        -- dsuh
            dsuh.usage_date     >= $l_start_dt
        and dsuh.usage_date      < $l_end_dt
    union
    select distinct
         ssuh.organization_name
        ,ssuh.region_name
        ,ssuh.account_name
        ,'~'                AS reader_account_name
    from
        &{l_raw_db}.&{l_raw_schema}.stage_storage_usage_history ssuh
    where
        -- ssuh
            ssuh.usage_date     >= $l_start_dt
        and ssuh.usage_date      < $l_end_dt
    union
    select distinct
         ach.organization_name
        ,ach.region_name
        ,ach.account_name
        ,'~'                AS reader_account_name
    from
        &{l_raw_db}.&{l_raw_schema}.automatic_clustering_history ach
    where
        -- ach
            ach.start_time        >= $l_start_dt
        and ach.start_time         < $l_end_dt
    union
    select distinct
         mvrh.organization_name
        ,mvrh.region_name
        ,mvrh.account_name
        ,'~'                AS reader_account_name
    from
        &{l_raw_db}.&{l_raw_schema}.materialized_view_refresh_history mvrh
    where
        -- mvrh
            mvrh.start_time        >= $l_start_dt
        and mvrh.start_time         < $l_end_dt
    union
    select distinct
         puh.organization_name
        ,puh.region_name
        ,puh.account_name
        ,'~'                AS reader_account_name
    from
        &{l_raw_db}.&{l_raw_schema}.pipe_usage_history puh
    where
        -- puh
            puh.start_time        >= $l_start_dt
        and puh.start_time         < $l_end_dt
    union
    select distinct
         soh.organization_name
        ,soh.region_name
        ,soh.account_name
        ,'~'                AS reader_account_name
    from
        &{l_raw_db}.&{l_raw_schema}.search_optimization_history soh
    where
        -- soh
            soh.start_time        >= $l_start_dt
        and soh.start_time         < $l_end_dt
    union
    select distinct
         mvrh.organization_name
        ,mvrh.region_name
        ,mvrh.account_name
        ,'~'                AS reader_account_name
    from
        &{l_raw_db}.&{l_raw_schema}.replication_usage_history mvrh
    where
        -- ach
            mvrh.start_time        >= $l_start_dt
        and mvrh.start_time         < $l_end_dt
    union
    select distinct
         dth.organization_name
        ,dth.region_name
        ,dth.account_name
        ,'~'                AS reader_account_name
    from
        &{l_raw_db}.&{l_raw_schema}.data_transfer_history dth
    where
        -- dth
            dth.start_time        >= $l_start_dt
        and dth.start_time         < $l_end_dt
)
,l_account_shk as
(
    select
        -- generate hash key
         sha1_binary( concat( la.organization_name
                             ,'|', la.region_name
                             ,'|', la.account_name
                             ,'|', la.reader_account_name
                            )
                    )                   as dw_account_shk
        --
        ,la.organization_name
        ,la.region_name
        ,la.account_name
        ,la.reader_account_name
    from
        l_account la
)
select
     las.dw_account_shk
    --
    ,las.organization_name
    ,las.region_name
    ,las.account_name
    ,las.reader_account_name
    --
    ,current_timestamp()            as dw_load_ts
    ,current_timestamp()            as dw_update_ts
from
    l_account_shk las
where
    las.dw_account_shk not in
    (
        select dw_account_shk from &{l_il_db}.&{l_il_schema}.cb_account_h
    )
order by
    2,3,4
;
