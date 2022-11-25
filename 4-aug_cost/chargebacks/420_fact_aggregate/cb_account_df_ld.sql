--------------------------------------------------------------------
--  Purpose: 
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------

--------------------------------------------------------------------
-- pull staged delta as a month boundary range
--
set (l_start_dt, l_end_dt ) = (select start_dt, end_dt from table( &{l_common_db}.&{l_common_schema}.dw_delta_date_range_f( 'all' ) ));

-- wrap delete and insert within a transaction so a failure with the insert doesn't leave the table without data for the delta range
begin;

    --------------------------------------------------------------------
    -- delete periods within the delta range
    --
    delete from 
        &{l_pl_db}.&{l_pl_schema}.cb_account_df
    where
            event_dt >= $l_start_dt
        and event_dt  < $l_end_dt
    ;
    
    --------------------------------------------------------------------
    -- load delta
    --
    insert into 
        &{l_pl_db}.&{l_pl_schema}.cb_account_df 
    select
         dw_account_shk
        ,dw_event_date_sid
        --
        ,event_dt
        ,max( compute_credit_cnt  )         as compute_credit_cnt  
        ,max( compute_cost_amt    )         as compute_cost_amt   
        ,max( storage_byte_cnt    )         as storage_byte_cnt   
        ,max( storage_cost_amt    )         as storage_cost_amt   
        ,max( data_xfer_byte_cnt  )         as data_xfer_byte_cnt 
        ,max( data_xfer_cost_amt  )         as data_xfer_cost_amt 
        ,max( total_cost_amt      )         as total_cost_amt     
        ,current_timestamp()                as dw_load_ts
        ,current_timestamp()                as dw_update_ts
    from
        (
        -- actuals
        select 
             cacm.dw_account_shk
            ,cacm.dw_event_date_sid
            --
            ,cacm.event_dt
            ,cacm.compute_credit_cnt 
            ,cacm.compute_cost_amt
            ,cacm.storage_byte_cnt
            ,cacm.storage_cost_amt
            ,cacm.data_xfer_byte_cnt
            ,cacm.data_xfer_cost_amt
            ,cacm.total_cost_amt
            ,0                                  as fcst_total_cost_amt
        from 
            &{l_il_db}.&{l_il_schema}.cb_account_consumption_ds cacm
        where
                cacm.event_dt >= $l_start_dt
            and cacm.event_dt  < $l_end_dt
        )
    group by
        2,1,3
    order by
        2,1
    ;

commit;
