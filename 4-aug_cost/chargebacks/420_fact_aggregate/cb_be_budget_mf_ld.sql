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
set (l_start_dt, l_end_dt ) = (select date_trunc( month, start_dt ), date_trunc( month, dateadd( month, 1, end_dt ) ) from table( &{l_common_db}.&{l_common_schema}.dw_delta_date_range_f( 'all' ) ));

-- wrap delete and insert within a transaction so a failure with the insert doesn't leave the table without data for the delta range
begin;

    --------------------------------------------------------------------
    -- delete periods within the delta range
    --
    delete from 
        &{l_pl_db}.&{l_pl_schema}.cb_be_budget_mf
    where
            event_month_dt >= $l_start_dt
        and event_month_dt  < $l_end_dt
    ;
    
    --------------------------------------------------------------------
    -- load delta
    --
    insert into 
        &{l_pl_db}.&{l_pl_schema}.cb_be_budget_mf 
    select
         dw_be_shk
        ,dw_event_date_sid
        --
        ,month_begin_dt
        ,max( compute_credit_cnt  )  as budget_compute_credit_cnt  
        ,current_timestamp()         as dw_load_ts
        ,current_timestamp()         as dw_update_ts
    from
        (
        -- actuals
        select 
             cacm.dw_be_shk
            ,cacm.dw_event_date_sid
            -- 
            ,cacm.month_begin_dt
            ,cacm.compute_credit_cnt 
            ,0                                  as fcst_total_cost_amt
        from 
            &{l_il_db}.&{l_il_schema}.be_budget_cost_s cacm
        where
               cacm.month_begin_dt >= $l_start_dt
           and cacm.month_begin_dt  < $l_end_dt
        )
    group by
        2,1,3
    order by
        2,1
    ;

commit;
