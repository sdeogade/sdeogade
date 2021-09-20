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
    &{l_il_db}.&{l_il_schema}.cb_resource_h
with l_resource as
(
    select distinct
         wmh.organization_name
        ,wmh.region_name
        ,wmh.account_name
        ,'~'                        as reader_account_name
        ,wmh.warehouse_name         as resource_name
        ,'warehouse'                as resource_type_cd
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
        ,wmh.account_name           as account_name
        ,wmh.reader_account_name    as reader_account_name
        ,wmh.warehouse_name         as resource_name
        ,'warehouse'                as resource_type_cd
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
        ,'~'                        as reader_account_name
        ,dsuh.database_name         as resource_name
        ,'database'                 as resource_type_cd
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
        ,'~'                        as reader_account_name
        ,'INTERNAL STAGE'           as resource_name
        ,'internal_stage'           as resource_type_cd
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
        ,'~'                        as reader_account_name
        ,ach.database_name          as resource_name
        ,'database'                 as resource_type_cd
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
        ,'~'                        as reader_account_name
        ,mvrh.database_name         as resource_name
        ,'database'                 as resource_type_cd
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
        ,'~'                        as reader_account_name
        ,puh.database_name          as resource_name
        ,'database'                 as resource_type_cd
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
        ,'~'                        as reader_account_name
        ,soh.database_name          as resource_name
        ,'database'                 as resource_type_cd
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
        ,'~'                        as reader_account_name
        ,mvrh.database_name         as resource_name
        ,'database'                 as resource_type_cd
    FROM 
        &{l_raw_db}.&{l_raw_schema}.replication_usage_history mvrh
    where
        -- mvrh
            mvrh.start_time        >= $l_start_dt
        and mvrh.start_time         < $l_end_dt
    union
    select distinct
         dth.organization_name
        ,dth.region_name
        ,dth.account_name
        ,'~'                        as reader_account_name
        ,concat( ifnull( dth.target_cloud, '?' ), '.', ifnull( dth.target_region, '?' ) )   as resource_name
        ,'cloudregion'                                                                      as resource_type_cd
    from
        &{l_raw_db}.&{l_raw_schema}.data_transfer_history dth
    where
        -- dth
            dth.start_time        >= $l_start_dt
        and dth.start_time         < $l_end_dt
)
,l_resource_shk as
(
    select
        -- generate hash key
         sha1_binary( concat( lr.organization_name
                             ,'|', lr.region_name
                             ,'|', lr.account_name
                             ,'|', lr.reader_account_name
                             ,'|', lr.resource_name
                             ,'|', lr.resource_type_cd
                            )
                    )                   as dw_resource_shk
        --
        ,lr.organization_name
        ,lr.region_name
        ,lr.account_name
        ,lr.reader_account_name
        ,lr.resource_name
        ,lr.resource_type_cd
    from
        l_resource lr
)
select
     lrs.dw_resource_shk
    --
    ,lrs.organization_name
    ,lrs.region_name
    ,lrs.account_name
    ,lrs.reader_account_name
    ,lrs.resource_name
    ,lrs.resource_type_cd
    --
    ,current_timestamp()            as dw_load_ts
    ,current_timestamp()            as dw_update_ts
from
    l_resource_shk lrs
where
    lrs.dw_resource_shk not in
    (
        select dw_resource_shk from &{l_il_db}.&{l_il_schema}.cb_resource_h
    )
order by
    2,3,4,5
;
