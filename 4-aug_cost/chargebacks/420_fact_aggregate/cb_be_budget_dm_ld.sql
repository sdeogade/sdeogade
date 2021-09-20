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
        &{l_pl_db}.&{l_pl_schema}.cb_be_budget_dm
    where
            event_month_dt >= $l_start_dt
        and event_month_dt  < $l_end_dt
    ;
    
    --------------------------------------------------------------------
    -- load delta
    --
    merge into 
        &{l_pl_db}.&{l_pl_schema}.cb_be_budget_dm bd 
	using  ( select
         dw_be_shk
		,be_name 
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
			,cacm.be_name
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
        3,2,1,4
    order by
        3,2,1 ) id 
    on
             bd.dw_be_shk = id.dw_be_shk
         and bd.dw_event_date_sid = id.dw_event_date_sid
         and bd.event_month_dt = id.month_begin_dt
         and bd.bgt_compute_credit_cnt = id.budget_compute_credit_cnt
	
    when matched then update
    set 
             bd.dw_be_shk = id.dw_be_shk
            ,bd.dw_event_date_sid = id.dw_event_date_sid
            ,bd.event_month_dt = id.month_begin_dt
            ,bd.bgt_compute_credit_cnt = id.budget_compute_credit_cnt
    
    when not matched then insert
       (dw_be_shk,dw_event_date_sid,event_month_dt,bgt_compute_credit_cnt,dw_load_ts,dw_update_ts)
    values (id.dw_be_shk,id.dw_event_date_sid,id.month_begin_dt,id.budget_compute_credit_cnt,current_timestamp(),current_timestamp())
    ;

commit;
