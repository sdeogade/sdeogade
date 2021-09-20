--------------------------------------------------------------------
--  Purpose:
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------

--------------------------------------------------------------------
-- delete insert pattern
--
-- the delta date range could theoretically ca--use change where a given
-- combination of pk values no longer exist, therefore the months
-- represented by the delta need to be reaggregated in their entirety
--

--------------------------------------------------------------------
-- pull staged delta as a month boundary range
--
set (l_start_dt, l_end_dt ) = (select date_trunc( month, start_dt ), date_trunc( month, dateadd( month, 1, end_dt ) ) from table( &{l_common_db}.&{l_common_schema}.dw_delta_date_range_f( 'all' ) ));

-- wrap delete and insert within a transaction so a failure with the insert doesn't leave the table without data for the delta range
begin;

    --------------------------------------------------------------------
    -- delete periods within the delta range
    --
    delete from
        &{l_il_db}.&{l_il_schema}.cb_resource_consumption_ms
    where
            event_month_dt >= $l_start_dt
        and event_month_dt  < $l_end_dt
    ;

    --------------------------------------------------------------------
    -- load delta
    --
    insert into
        &{l_il_db}.&{l_il_schema}.cb_resource_consumption_ms
    with l_resource_consumption as
    (
        --
        -- warehouse metering
        --
        select
             cah.dw_account_shk
            ,csh.dw_resource_shk
            ,cstl.dw_service_type_shk
            ,&{l_common_db}.&{l_common_schema}.date_sid_f( date_trunc( month, wmh.start_time ) )    as dw_event_date_sid
            ,date_trunc( month, wmh.start_time )                  as event_month_dt
            --
            ,sum( wmh.credits_used )                            as compute_credit_cnt
            ,0                                                  as storage_byte_cnt
            ,0                                                  as data_xfer_byte_cnt
        from
            &{l_raw_db}.&{l_raw_schema}.warehouse_metering_history wmh
            join &{l_il_db}.&{l_il_schema}.cb_resource_h csh on
                    csh.organization_name   = wmh.organization_name
                and csh.account_name        = wmh.account_name
                and csh.reader_account_name = '~'
                and csh.region_name         = wmh.region_name
                and csh.resource_name       = wmh.warehouse_name
            join &{l_il_db}.&{l_il_schema}.cb_account_h cah on
                    cah.organization_name   = wmh.organization_name
                and cah.account_name        = wmh.account_name
                and cah.reader_account_name = '~'
                and cah.region_name         = wmh.region_name
            cross join &{l_raw_db}.&{l_raw_schema}.cb_service_type_lkp cstl
        where
            -- wmh
                wmh.start_time        >= $l_start_dt
            and wmh.start_time         < $l_end_dt
            -- csh
            and csh.resource_type_cd = 'warehouse'
            -- cstl
            and cstl.service_type_cd = 'WAREHOUSE_METERING'
        group by
            1,2,3,4,5
        union all
        --
        -- reader account warehouse metering
        --
        select
             cah.dw_account_shk
            ,csh.dw_resource_shk
            ,cstl.dw_service_type_shk
            ,&{l_common_db}.&{l_common_schema}.date_sid_f( date_trunc( month, wmh.start_time ) )    as dw_event_date_sid
            ,date_trunc( month, wmh.start_time )                  as event_month_dt
            --
            ,sum( wmh.credits_used )                            as compute_credit_cnt
            ,0                                                  as storage_byte_cnt
            ,0                                                  as data_xfer_byte_cnt
        from
            &{l_raw_db}.&{l_raw_schema}.warehouse_metering_history_reader wmh
            join &{l_il_db}.&{l_il_schema}.cb_resource_h csh on
                    csh.organization_name   = wmh.organization_name
                and csh.account_name        = wmh.account_name
                and csh.reader_account_name = wmh.reader_account_name
                and csh.region_name         = wmh.region_name
                and csh.resource_name       = wmh.warehouse_name
            join &{l_il_db}.&{l_il_schema}.cb_account_h cah on
                    cah.organization_name   = wmh.organization_name
                and cah.account_name        = wmh.account_name
                and cah.reader_account_name = wmh.reader_account_name
                and cah.region_name         = wmh.region_name
            cross join &{l_raw_db}.&{l_raw_schema}.cb_service_type_lkp cstl
        where
            -- wmh
                wmh.start_time        >= $l_start_dt
            and wmh.start_time         < $l_end_dt
            -- csh
            and csh.resource_type_cd = 'warehouse'
            -- cstl
            and cstl.service_type_cd = 'WAREHOUSE_METERING_READER'
        group by
            1,2,3,4,5
        union all
        --
        -- automatic clustering
        --
        select
             cah.dw_account_shk
            ,csh.dw_resource_shk
            ,cstl.dw_service_type_shk
            ,&{l_common_db}.&{l_common_schema}.date_sid_f( date_trunc( month, ach.start_time ) )    as dw_event_date_sid
            ,date_trunc( month, ach.start_time )                  as event_month_dt
            --
            ,sum( ach.credits_used )                            as compute_credit_cnt
            ,0                                                  as storage_byte_cnt
            ,0                                                  as data_xfer_byte_cnt
        from
            &{l_raw_db}.&{l_raw_schema}.automatic_clustering_history ach
            join &{l_il_db}.&{l_il_schema}.cb_resource_h csh on
                    csh.organization_name   = ach.organization_name
                and csh.account_name        = ach.account_name
                and csh.reader_account_name = '~'
                and csh.region_name         = ach.region_name
                and csh.resource_name       = ach.database_name
            join &{l_il_db}.&{l_il_schema}.cb_account_h cah on
                    cah.organization_name   = ach.organization_name
                and cah.account_name        = ach.account_name
                and cah.reader_account_name = '~'
                and cah.region_name         = ach.region_name
            cross join &{l_raw_db}.&{l_raw_schema}.cb_service_type_lkp cstl
        where
            -- ach
                ach.start_time        >= $l_start_dt
            and ach.start_time         < $l_end_dt
            -- csh
            and csh.resource_type_cd = 'database'
            -- cstl
            and cstl.service_type_cd = 'AUTOMATIC_CLUSTERING'
        group by
            1,2,3,4,5
        union all
        --
        -- materialized view refresh
        --
        select
             cah.dw_account_shk
            ,csh.dw_resource_shk
            ,cstl.dw_service_type_shk
            ,&{l_common_db}.&{l_common_schema}.date_sid_f( date_trunc( month, mvrh.start_time ) )    as dw_event_date_sid
            ,date_trunc( month, mvrh.start_time )               as event_month_dt
            --
            ,sum( mvrh.credits_used )                           as compute_credit_cnt
            ,0                                                  as storage_byte_cnt
            ,0                                                  as data_xfer_byte_cnt
        from
            &{l_raw_db}.&{l_raw_schema}.materialized_view_refresh_history mvrh
            join &{l_il_db}.&{l_il_schema}.cb_resource_h csh on
                    csh.organization_name   = mvrh.organization_name
                and csh.account_name        = mvrh.account_name
                and csh.reader_account_name = '~'
                and csh.region_name         = mvrh.region_name
                and csh.resource_name       = mvrh.database_name
            join &{l_il_db}.&{l_il_schema}.cb_account_h cah on
                    cah.organization_name   = mvrh.organization_name
                and cah.account_name        = mvrh.account_name
                and cah.reader_account_name = '~'
                and cah.region_name         = mvrh.region_name
            cross join &{l_raw_db}.&{l_raw_schema}.cb_service_type_lkp cstl
        where
            -- mvrh
                mvrh.start_time     >= $l_start_dt
            and mvrh.start_time      < $l_end_dt
            -- csh
            and csh.resource_type_cd = 'database'
            -- cstl
            and cstl.service_type_cd = 'MATERIALIZED_VIEW'
        group by
            1,2,3,4,5
        union all
        --
        -- pipe usage
        --
        select
             cah.dw_account_shk
            ,csh.dw_resource_shk
            ,cstl.dw_service_type_shk
            ,&{l_common_db}.&{l_common_schema}.date_sid_f( date_trunc( month, puh.start_time ) )    as dw_event_date_sid
            ,date_trunc( month, puh.start_time )                as event_month_dt
            --
            ,sum( puh.credits_used )                            as compute_credit_cnt
            ,0                                                  as storage_byte_cnt
            ,0                                                  as data_xfer_byte_cnt
        from
            &{l_raw_db}.&{l_raw_schema}.pipe_usage_history puh
            join &{l_il_db}.&{l_il_schema}.cb_resource_h csh on
                    csh.organization_name   = puh.organization_name
                and csh.account_name        = puh.account_name
                and csh.reader_account_name = '~'
                and csh.region_name         = puh.region_name
                and csh.resource_name       = puh.database_name
            join &{l_il_db}.&{l_il_schema}.cb_account_h cah on
                    cah.organization_name   = puh.organization_name
                and cah.account_name        = puh.account_name
                and cah.reader_account_name = '~'
                and cah.region_name         = puh.region_name
            cross join &{l_raw_db}.&{l_raw_schema}.cb_service_type_lkp cstl
        where
            -- puh
                puh.start_time     >= $l_start_dt
            and puh.start_time      < $l_end_dt
            -- csh
            and csh.resource_type_cd = 'database'
            -- cstl
            and cstl.service_type_cd = 'PIPE'
        group by
            1,2,3,4,5
        union all
        --
        -- search optimization
        --
        select
             cah.dw_account_shk
            ,csh.dw_resource_shk
            ,cstl.dw_service_type_shk
            ,&{l_common_db}.&{l_common_schema}.date_sid_f( date_trunc( month, soh.start_time ) )    as dw_event_date_sid
            ,date_trunc( month, soh.start_time )                as event_month_dt
            --
            ,sum( soh.credits_used )                            as compute_credit_cnt
            ,0                                                  as storage_byte_cnt
            ,0                                                  as data_xfer_byte_cnt
        from
            &{l_raw_db}.&{l_raw_schema}.search_optimization_history soh
            join &{l_il_db}.&{l_il_schema}.cb_resource_h csh on
                    csh.organization_name   = soh.organization_name
                and csh.account_name        = soh.account_name
                and csh.reader_account_name = '~'
                and csh.region_name         = soh.region_name
                and csh.resource_name       = soh.database_name
            join &{l_il_db}.&{l_il_schema}.cb_account_h cah on
                    cah.organization_name   = soh.organization_name
                and cah.account_name        = soh.account_name
                and cah.reader_account_name = '~'
                and cah.region_name         = soh.region_name
            cross join &{l_raw_db}.&{l_raw_schema}.cb_service_type_lkp cstl
        where
            -- soh
                soh.start_time      >= $l_start_dt
            and soh.start_time       < $l_end_dt
            -- csh
            and csh.resource_type_cd = 'database'
            -- cstl
            and cstl.service_type_cd = 'SEARCH_OPTIMIZATION'
        group by
            1,2,3,4,5
        union all
        --
        -- replication usage history credits used
        --
        select
             cah.dw_account_shk
            ,csh.dw_resource_shk
            ,cstl.dw_service_type_shk
            ,&{l_common_db}.&{l_common_schema}.date_sid_f( date_trunc( month, ruh.start_time ) )    as dw_event_date_sid
            ,date_trunc( month, ruh.start_time )                as event_month_dt
            --
            ,sum( ruh.credits_used )                            as compute_credit_cnt
            ,0                                                  as storage_byte_cnt
            ,0                                                  as data_xfer_byte_cnt
        from
            &{l_raw_db}.&{l_raw_schema}.replication_usage_history ruh
            join &{l_il_db}.&{l_il_schema}.cb_resource_h csh on
                    csh.organization_name   = ruh.organization_name
                and csh.account_name        = ruh.account_name
                and csh.reader_account_name = '~'
                and csh.region_name         = ruh.region_name
                and csh.resource_name       = ruh.database_name
            join &{l_il_db}.&{l_il_schema}.cb_account_h cah on
                    cah.organization_name   = ruh.organization_name
                and cah.account_name        = ruh.account_name
                and cah.reader_account_name = '~'
                and cah.region_name         = ruh.region_name
            cross join &{l_raw_db}.&{l_raw_schema}.cb_service_type_lkp cstl
        where
            -- ruh
                ruh.start_time      >= $l_start_dt
            and ruh.start_time       < $l_end_dt
            -- csh
            and csh.resource_type_cd = 'database'
            -- cstl
            and cstl.service_type_cd = 'REPLICATION_METERING'
        group by
            1,2,3,4,5
        union all
        --
        -- replication usage history bytes transferred
        --
        select
             cah.dw_account_shk
            ,csh.dw_resource_shk
            ,cstl.dw_service_type_shk
            ,&{l_common_db}.&{l_common_schema}.date_sid_f( date_trunc( month, ruh.start_time ) )    as dw_event_date_sid
            ,date_trunc( month, ruh.start_time )                as event_month_dt
            --
            ,0                                                  as compute_credit_cnt
            ,0                                                  as storage_byte_cnt
            ,sum( ruh.bytes_transferred )                       as data_xfer_byte_cnt
        from
            &{l_raw_db}.&{l_raw_schema}.replication_usage_history ruh
            join &{l_il_db}.&{l_il_schema}.cb_resource_h csh on
                    csh.organization_name   = ruh.organization_name
                and csh.account_name        = ruh.account_name
                and csh.reader_account_name = '~'
                and csh.region_name         = ruh.region_name
                and csh.resource_name       = ruh.database_name
            join &{l_il_db}.&{l_il_schema}.cb_account_h cah on
                    cah.organization_name   = ruh.organization_name
                and cah.account_name        = ruh.account_name
                and cah.reader_account_name = '~'
                and cah.region_name         = ruh.region_name
            cross join &{l_raw_db}.&{l_raw_schema}.cb_service_type_lkp cstl
        where
            -- ruh
                ruh.start_time      >= $l_start_dt
            and ruh.start_time       < $l_end_dt
            -- csh
            and csh.resource_type_cd = 'database'
            -- cstl
            and cstl.service_type_cd = 'REPLICATION_DATA_TRANSFER'
        group by
            1,2,3,4,5
        union all
        --
        -- database storage
        --
        select
             cah.dw_account_shk
            ,csh.dw_resource_shk
            ,cstl.dw_service_type_shk
            ,&{l_common_db}.&{l_common_schema}.date_sid_f( date_trunc( month, dsuh.usage_date ) )     as dw_event_date_sid
            ,date_trunc( month, dsuh.usage_date )                   as event_month_dt
            --
            ,0                                                      as compute_credit_cnt
            ,avg(  dsuh.average_database_bytes
                 + dsuh.average_failsafe_bytes )                    as storage_byte_cnt
            ,0                                                      as data_xfer_byte_cnt
        from
            &{l_raw_db}.&{l_raw_schema}.database_storage_usage_history dsuh
            join &{l_il_db}.&{l_il_schema}.cb_resource_h csh on
                    csh.organization_name   = dsuh.organization_name
                and csh.account_name        = dsuh.account_name
                and csh.reader_account_name = '~'
                and csh.region_name         = dsuh.region_name
                and csh.resource_name       = dsuh.database_name
            join &{l_il_db}.&{l_il_schema}.cb_account_h cah on
                    cah.organization_name   = dsuh.organization_name
                and cah.account_name        = dsuh.account_name
                and cah.reader_account_name = '~'
                and cah.region_name         = dsuh.region_name
            cross join &{l_raw_db}.&{l_raw_schema}.cb_service_type_lkp cstl
        where
            -- dsuh
                dsuh.usage_date     >= $l_start_dt
            and dsuh.usage_date      < $l_end_dt
            -- csh
            and csh.resource_type_cd = 'database'
            -- cstl
            and cstl.service_type_cd = 'STORAGE_DATABASE'
        group by
            1,2,3,4,5
        union all
        --
        -- stage storage
        --
        select
             cah.dw_account_shk
            ,csh.dw_resource_shk
            ,cstl.dw_service_type_shk
            ,&{l_common_db}.&{l_common_schema}.date_sid_f( date_trunc( month, ssuh.usage_date ) )     as dw_event_date_sid
            ,date_trunc( month, ssuh.usage_date )                   as event_month_dt
            --
            ,0                                                      as compute_credit_cnt
            ,avg(  ssuh.average_stage_bytes )                       as storage_byte_cnt
            ,0                                                      as data_xfer_byte_cnt
        from
            &{l_raw_db}.&{l_raw_schema}.stage_storage_usage_history ssuh
            join &{l_il_db}.&{l_il_schema}.cb_resource_h csh on
                    csh.organization_name   = ssuh.organization_name
                and csh.account_name        = ssuh.account_name
                and csh.reader_account_name = '~'
                and csh.region_name         = ssuh.region_name
                and csh.resource_name       = 'INTERNAL STAGE'
            join &{l_il_db}.&{l_il_schema}.cb_account_h cah on
                    cah.organization_name   = ssuh.organization_name
                and cah.account_name        = ssuh.account_name
                and cah.reader_account_name = '~'
                and cah.region_name         = ssuh.region_name
            cross join &{l_raw_db}.&{l_raw_schema}.cb_service_type_lkp cstl
        where
            -- ssuh
                ssuh.usage_date     >= $l_start_dt
            and ssuh.usage_date      < $l_end_dt
            -- csh
            and csh.resource_type_cd = 'internal_stage'
            -- cstl
            and cstl.service_type_cd = 'STORAGE_STAGE'
        group by
            1,2,3,4,5
        union all
        --
        -- data transfer
        --
        select
             cah.dw_account_shk
            ,csh.dw_resource_shk
            ,cstl.dw_service_type_shk
            ,&{l_common_db}.&{l_common_schema}.date_sid_f( date_trunc( month, dth.start_time ) )        as dw_event_date_sid
            ,date_trunc( month, dth.start_time )                      as event_month_dt
            --
            ,0                                                      as compute_credit_cnt
            ,0                                                      as storage_byte_cnt
            ,sum( dth.bytes_transferred )                           as data_xfer_byte_cnt
        from
            &{l_raw_db}.&{l_raw_schema}.data_transfer_history dth
            join &{l_il_db}.&{l_il_schema}.cb_resource_h csh on
                    csh.organization_name   = dth.organization_name
                and csh.account_name        = dth.account_name
                and csh.reader_account_name = '~'
                and csh.region_name         = dth.region_name
                and csh.resource_name       = concat( ifnull( dth.target_cloud, '?' ), '.', ifnull( dth.target_region, '?' ) )
            join &{l_il_db}.&{l_il_schema}.cb_account_h cah on
                    cah.organization_name   = dth.organization_name
                and cah.account_name        = dth.account_name
                and cah.reader_account_name = '~'
                and cah.region_name         = dth.region_name
            cross join &{l_raw_db}.&{l_raw_schema}.cb_service_type_lkp cstl
        where
            -- dth
                dth.start_time        >= $l_start_dt
            and dth.start_time         < $l_end_dt
            -- csh
            and csh.resource_type_cd = 'cloudregion'
            -- cstl
            and cstl.service_type_cd = 'DATA_TRANSFER'
        group by
            1,2,3,4,5
    )
    select
         lrc.dw_account_shk
        ,lrc.dw_resource_shk
        ,lrc.dw_service_type_shk
        ,lrc.dw_event_date_sid
        ,lrc.event_month_dt
        --
        ,lrc.compute_credit_cnt
        ,lrc.compute_credit_cnt * &{l_common_db}.&{l_common_schema}.sf_compute_cost_f( lrc.dw_account_shk, lrc.event_month_dt )
                                                                as compute_cost_amt
        ,lrc.storage_byte_cnt
        ,lrc.storage_byte_cnt / pow( 1024, 4 )                  as storage_tb_cnt
        ,(lrc.storage_byte_cnt / pow( 1024, 4 )) * &{l_common_db}.&{l_common_schema}.sf_storage_cost_f( lrc.dw_account_shk, lrc.event_month_dt )
                                                                as storage_cost_amt
        ,lrc.data_xfer_byte_cnt
        ,lrc.data_xfer_byte_cnt / pow( 1024, 4 )                as data_xfer_tb_cnt
        ,(lrc.data_xfer_byte_cnt / pow( 1024, 4 )) * &{l_common_db}.&{l_common_schema}.sf_data_xfer_cost_f( lrc.dw_account_shk, lrc.event_month_dt )
                                                                as data_xfer_cost_amt
        ,compute_cost_amt
         + storage_cost_amt
         + data_xfer_cost_amt                                   as total_cost_amt
        --
        ,current_timestamp()                                    as dw_load_ts
    from
        l_resource_consumption lrc
    where
        not exists
        (
            select 1 from &{l_il_db}.&{l_il_schema}.cb_resource_consumption_ms where event_month_dt >= $l_start_dt and event_month_dt < $l_end_dt
        )
    order by
        3
    ;

commit;
