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
-- using a delete/insert pattern because all atomic records must be
-- reprocessed within a given period to ensure allocation is correct
--

--------------------------------------------------------------------
-- pull staged delta as day boundary range
--
set (l_start_dt, l_end_dt ) = (select start_dt, dateadd( day, 1, end_dt ) from table( &{l_common_db}.&{l_common_schema}.dw_delta_date_range_f( 'all' ) ));

-- pull max number of hour periods that some jobs will need to be expanded across for hourly cost allocation
set l_hour_cnt = (
                 SELECT max( hour_no ) + 1 AS hour_cnt
                 FROM
                     (
		                 select
		                     datediff( hour, date_trunc( hour, qh.start_time ), qh.end_time ) as hour_no
		                 from
		                     &{l_raw_db}.&{l_raw_schema}.query_history qh
		                 where
		                         qh.start_time >= $l_start_dt
		                     and qh.start_time  < $l_end_dt
		                 UNION 
		                 select
		                     datediff( hour, date_trunc( hour, qh.start_time ), qh.end_time ) as hour_no
		                 from
		                     &{l_raw_db}.&{l_raw_schema}.query_history_reader qh
		                 where
		                         qh.start_time >= $l_start_dt
		                     and qh.start_time  < $l_end_dt
		             )
                 )
;

-- wrap delete and insert within a transaction so a failure with the insert doesn't leave the table without data for the delta range
begin;

    --------------------------------------------------------------------
    -- delete periods within the delta range
    --
    delete from
        &{l_il_db}.&{l_il_schema}.cb_job_credits_s
    where
            start_time >= $l_start_dt
        and start_time  < $l_end_dt
    ;

    --------------------------------------------------------------------
    -- load delta
    --
    insert into
        &{l_il_db}.&{l_il_schema}.cb_job_credits_s
    with l_hour as
    (
        -- generate hour rows
        select
            row_number() over( order by seq4() )  as seq_no
        from
            table( generator( rowcount => $l_hour_cnt ) ) 
    )
    ,l_period as
    (
        -- generate period rows with corresponding hour rows
        -- for example: hour_cnt 1 has rows 1; hour_cnt 2 has rows 1,2; hour_cnt 3 has rows 1,2,3; etc.
        -- these will be used to split jobs that ran for muliple hours across those periods
        select
             a.seq_no as hour_cnt
            ,b.seq_no as hour_seq_no
        from
            l_hour a
            cross join l_hour b
        where
            b.seq_no <= a.seq_no
    )
    ,l_job as
    (
        select
             qh.dw_query_history_shk
            ,qh.organization_name
            ,qh.region_name
            ,qh.account_name
            ,'~'                     as reader_account_name
            ,qh.warehouse_name
            ,qh.query_id
            ,qh.start_time
            ,qh.end_time
            -- 
            -- derivations
            --   move start time to after queueing completion
            --   set period start and end hour times
            --   derive hours spanned from period start and end
            --
            ,case
                when qh.warehouse_size is not null
                then 1
                else 0
             end                                                        as credits_consumed_bt
            ,dateadd( ms, qh.queued_provisioning_time + qh.queued_repair_time + qh.queued_overload_time, qh.start_time )    as adj_job_start_ts
            ,date_trunc( hour, adj_job_start_ts )                       as job_period_start_ts
            ,dateadd( hour, 1, date_trunc( hour, qh.end_time ) )        as job_period_end_ts
            ,datediff( hour, job_period_start_ts, job_period_end_ts )   as job_hour_cnt
        from
            &{l_raw_db}.&{l_raw_schema}.query_history qh
        where
            -- qh
                qh.start_time >= $l_start_dt
            and qh.start_time  < $l_end_dt
        --
        UNION ALL
        -- 
        -- reader account query history
        --
        select
             qh.dw_query_history_shk
            ,qh.organization_name
            ,qh.region_name
            ,qh.account_name
            ,qh.reader_account_name
            ,qh.warehouse_name
            ,qh.query_id
            ,qh.start_time
            ,qh.end_time
            -- 
            -- derivations
            --   move start time to after queueing completion
            --   set period start and end hour times
            --   derive hours spanned from period start and end
            --
            ,case
                when qh.warehouse_size is not null
                then 1
                else 0
             end                                                        as credits_consumed_bt
            ,dateadd( ms, qh.queued_provisioning_time + qh.queued_repair_time + qh.queued_overload_time, qh.start_time )    as adj_job_start_ts
            ,date_trunc( hour, adj_job_start_ts )                       as job_period_start_ts
            ,dateadd( hour, 1, date_trunc( hour, qh.end_time ) )        as job_period_end_ts
            ,datediff( hour, job_period_start_ts, job_period_end_ts )   as job_hour_cnt
        from
            &{l_raw_db}.&{l_raw_schema}.query_history_reader qh
        where
            -- qh
                qh.start_time >= $l_start_dt
            and qh.start_time  < $l_end_dt
    )
    ,l_job_period as
    (
        -- split query history rows across the hour periods that they span and calc percent of execution time within the hour for each job
        select
             lj.*
            ,lp.*
            -- derive period duration
            ,dateadd( hour, lp.hour_seq_no - 1, lj.job_period_start_ts ) as period_start_ts
            ,dateadd( hour, lp.hour_seq_no,     lj.job_period_start_ts ) as period_end_ts
            ,case
                when lp.hour_seq_no = 1
                then datediff( ms, lj.adj_job_start_ts, least( lj.end_time, period_end_ts ) )
                when lp.hour_seq_no < lp.hour_cnt
                then datediff( ms, period_start_ts, period_end_ts )
                when lp.hour_seq_no = lp.hour_cnt
                then datediff( ms, period_start_ts, lj.end_time )
                else -1
             end                        as period_duration
            -- adjust to zero if execution did not consume credits on the warehouse
            ,period_duration * lj.credits_consumed_bt   as adj_period_duration
            --
            ,ratio_to_report( to_number( adj_period_duration, 38, 10 ) ) over( partition by lj.organization_name, lj.region_name, lj.account_name, lj.warehouse_name, period_start_ts ) as period_duration_pct
        from
            l_job lj
            join l_period lp on
                lp.hour_cnt = lj.job_hour_cnt
    )
    ,l_job_cost as
    (
        -- allocate warehouse credits across jobs that spanned a given hour period based on percent of execution time
        select
             ljp.*
            ,nvl( lwm.credits_used_compute, 0 )                         as credits_used_compute
            ,nvl( credits_used_compute * ljp.period_duration_pct, 0 )   as job_credits_used_compute
        from
            l_job_period ljp
            left join &{l_raw_db}.&{l_raw_schema}.warehouse_metering_history lwm on
                    lwm.organization_name   = ljp.organization_name
                and lwm.region_name         = ljp.region_name
                and lwm.account_name        = ljp.account_name
                and lwm.warehouse_name      = ljp.warehouse_name
                and lwm.start_time          = ljp.period_start_ts
        where
            (
                lwm.start_time >= $l_start_dt
            and lwm.start_time  < $l_end_dt
            )
            -- bring in all query history rows
            or lwm.warehouse_name is NULL
        --
        UNION all
        --
        -- reader account warehouse metering
        --
        select
             ljp.*
            ,nvl( lwm.credits_used_compute, 0 )                         as credits_used_compute
            ,nvl( credits_used_compute * ljp.period_duration_pct, 0 )   as job_credits_used_compute
        from
            l_job_period ljp
            left join &{l_raw_db}.&{l_raw_schema}.warehouse_metering_history_reader lwm on
                    lwm.organization_name   = ljp.organization_name
                and lwm.region_name         = ljp.region_name
                and lwm.account_name        = ljp.account_name
                AND lwm.reader_account_name = ljp.reader_account_name
                and lwm.warehouse_name      = ljp.warehouse_name
                and lwm.start_time          = ljp.period_start_ts
        where
            (
                lwm.start_time >= $l_start_dt
            and lwm.start_time  < $l_end_dt
            )
            -- bring in all query history rows
            or lwm.warehouse_name is null
    )
    select
         ljp.dw_query_history_shk
        ,ljp.start_time 
        ,ljp.hour_seq_no
        --
        ,ljp.organization_name
        ,ljp.account_name
        ,ljp.reader_account_name
        ,ljp.region_name
        ,ljp.warehouse_name
        ,ljp.job_credits_used_compute
        --
        ,ljp.job_hour_cnt
        ,ljp.credits_used_compute
        ,ljp.period_duration_pct
        ,ljp.period_duration
        ,ljp.adj_period_duration
        ,ljp.credits_consumed_bt
        ,ljp.job_period_start_ts
        ,ljp.period_start_ts 
        ,ljp.period_end_ts
        ,ljp.adj_job_start_ts
        --
        ,current_timestamp()        as dw_load_ts
    from
        l_job_cost ljp
    where
        not exists
        (
            select 1 from &{l_il_db}.&{l_il_schema}.cb_job_credits_s where start_time >= $l_start_dt and start_time < $l_end_dt
        )
    order by
        ljp.start_time
    ;

commit;
