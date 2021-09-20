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
        &{l_pl_db}.&{l_pl_schema}.cb_resource_mf
    where
            event_month_dt >= $l_start_dt
        and event_month_dt  < $l_end_dt
    ;
    
    --------------------------------------------------------------------
    -- load delta
    --
    insert into 
        &{l_pl_db}.&{l_pl_schema}.cb_resource_mf
    select 
         crcm.dw_resource_shk
        ,crcm.dw_service_type_shk
        ,crcm.dw_event_date_sid
        --
        ,crcm.event_month_dt
        ,crcm.compute_credit_cnt
        ,crcm.compute_cost_amt
        ,crcm.storage_byte_cnt
        ,crcm.storage_cost_amt
        ,crcm.data_xfer_byte_cnt
        ,crcm.data_xfer_cost_amt
        --
        ,current_timestamp()                                as dw_load_ts
    from 
        &{l_il_db}.&{l_il_schema}.cb_resource_consumption_ms crcm
    where
        not exists 
        (
            select 1 from &{l_pl_db}.&{l_pl_schema}.cb_resource_mf where event_month_dt >= $l_start_dt and event_month_dt < $l_end_dt
        )
        and crcm.event_month_dt >= $l_start_dt
        and crcm.event_month_dt < $l_end_dt
    order by
        3,1,2
    ;

commit;


