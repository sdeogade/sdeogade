------------------------------------------------------------------------------
-- sample queries against resulting fact table structure
--
-- note: this is just created as an example and will need to be tailed to the
--       specific needs of DMED.

USE ROLE mwebster_dev_engineer_fr;
USE ROLE mwebster_dev_sysadmin_fr;
USE WAREHOUSE mwebster_dev_service_elt_wh;
use schema mwebster_dev_pl_db.main;

show databases like 'mwebster%';
SHOW ROLES LIKE 'mwebster%';

-- dims
SELECT * FROM cb_account_dm;
SELECT * FROM cb_resource_dm;
SELECT * FROM cb_service_type_dm;
SELECT * FROM date_dm;

-- facts
SELECT * FROM cb_account_mf;
SELECT * FROM cb_resource_mf;

USE DATABASE mwebster_dev_rl_snowflake_db;
USE SCHEMA account_usage;
SELECT * FROM be_resource_mapping_lkp;

--
-- cost allocation to query history jobs
--

set l_start_dt = to_timestamp( '05/15/2021', 'mm/dd/yyyy' );
set l_end_dt   = to_timestamp( '05/17/2021', 'mm/dd/yyyy' );

select $l_start_dt, $l_end_dt;

-- validation showing aggregation of job credit allocations matching warehouse_metering_history. 
-- validation showing aggregation of job credit allocations matching warehouse_metering_history. 
with l_warehouse_metering_history as
(
    select
         wmh.organization_name
        ,wmh.region_name
        ,wmh.account_name
        ,'~'                 as reader_account_name
        ,wmh.warehouse_name
        ,wmh.start_time
        ,wmh.credits_used_compute
    from
        mwebster_dev_rl_snowflake_db.account_usage.warehouse_metering_history wmh
    where
            wmh.start_time >= $l_start_dt
        and wmh.start_time  < $l_end_dt
    --
    union all
    select
         wmh.organization_name
        ,wmh.region_name
        ,wmh.account_name
        ,wmh.reader_account_name
        ,wmh.warehouse_name
        ,wmh.start_time
        ,wmh.credits_used_compute
    from
        mwebster_dev_rl_snowflake_db.account_usage.warehouse_metering_history_reader wmh
    where
            wmh.start_time >= $l_start_dt
        and wmh.start_time  < $l_end_dt
)
,l_job_credits as
(
    select
         cjcs.organization_name
        ,cjcs.region_name
        ,cjcs.account_name
        ,cjcs.reader_account_name
        ,cjcs.warehouse_name
        ,cjcs.period_start_ts
        ,sum( cjcs.job_credits_used_compute )   as job_credits_used_compute
    from
        mwebster_dev_il_db.main.cb_job_credits_s cjcs
    where
            cjcs.start_time >= $l_start_dt
        and cjcs.start_time  < $l_end_dt
    group by
        1,2,3,4,5,6
)
select 
     lwmh.organization_name
    ,lwmh.region_name
    ,lwmh.account_name
    ,lwhm.reader_account_name
    ,lwmh.warehouse_name
    ,lwmh.start_time
    ,lwmh.credits_used_compute
    ,ljc.job_credits_used_compute
from
    l_warehouse_metering_history lwmh
    left join l_job_credits ljc on
            ljc.organization_name   = lwmh.organization_name
        and ljc.region_name         = lwmh.region_name
        and ljc.account_name        = lwmh.account_name
        AND ljc.reader_account_name = lwmh.reader_account_name
        and ljc.warehouse_name      = lwmh.warehouse_name
        and ljc.period_start_ts     = lwmh.start_time
order by
     1,2,3,4,5,6
;


show warehouses like 'GSITZMAN_WH';
show warehouses like 'GSDMED%';

use warehouse mwebster_dev_adhoc_all_wh;

use role sysadmin;

-- aggregate job credits at role and user level
with l_job_cost as
(
    -- reconsolidate into 1 row per query id
    select
         dw_query_history_shk
        ,start_time
        ,sum( job_credits_used_compute )    as job_credits_used_compute
    from
        mwebster_dev_il_db.main.cb_job_credits_s
    where
            start_time >= $l_start_dt
        and start_time  < $l_end_dt
    group by
        1,2
)
select
     qh.user_name
    ,qh.role_name
    ,count( distinct qh.dw_query_history_shk )  as job_cnt
    ,sum( cjcs.job_credits_used_compute )       as job_credits_used_compute
    ,sum( qh.credits_used_cloud_services )      as credits_used_cloud_services
from
    l_job_cost cjcs
    join mwebster_dev_rl_snowflake_db.account_usage.query_history qh on
        qh.dw_query_history_shk = cjcs.dw_query_history_shk
where
    -- cjcs
        cjcs.start_time >= $l_start_dt
    and cjcs.start_time  < $l_end_dt
    -- qh
    and qh.start_time  >= $l_start_dt
    and qh.start_time   < $l_end_dt
group by
    1,2
order by
    4 desc nulls last
;

-------------------------------------------------------------------------------
-- dimensional model sample queries
--
use schema mwebster_dev_pl_db.main;

--
-- account fact with forecast
--
select
     cad.organization_name
    ,cad.account_name
    ,cad.reader_account_name
    ,cad.region_name
    ,dd.cal_month_dt
    ,sum( camf.total_cost_amt )          as total_cost_amt
    ,sum( camf.fcst_total_cost_amt )     as fcst_total_cost_amt
    --
    ,sum( camf.compute_credit_cnt )      as compute_credit_cnt
    ,sum( camf.compute_cost_amt )        as compute_cost_amt
    ,sum( camf.storage_byte_cnt )        as storage_byte_cnt
    ,sum( camf.storage_cost_amt )        as storage_cost_amt
    ,sum( camf.data_xfer_byte_cnt )      as data_xfer_byte_cnt
    ,sum( camf.data_xfer_cost_amt )      as data_xfer_cost_amt
from
    cb_account_mf camf
    join cb_account_dm cad on
        cad.dw_account_shk = camf.dw_account_shk
    join date_dm dd on
        dd.dw_date_sid = camf.dw_event_date_sid
group by
    1,2,3,4,5
order BY
    1,2,3,4,5
;

--
-- resource fact
-- aggregation by team, resource type, account, be name
--
select
     crd.organization_name
    ,crd.be_name
    ,crd.account_name
    ,crd.reader_account_name
    ,crd.resource_type_cd
    ,crd.resource_name
    ,crd.team_name
    ,cstd.service_type_name
    ,sum( crmf.compute_credit_cnt )      as compute_credit_cnt
    ,sum( crmf.compute_cost_amt )        as compute_cost_amt
    ,sum( crmf.storage_byte_cnt )        as storage_byte_cnt
    ,sum( crmf.storage_cost_amt )        as storage_cost_amt
    ,sum( crmf.data_xfer_byte_cnt )      as data_xfer_byte_cnt
    ,sum( crmf.data_xfer_cost_amt )      as data_xfer_cost_amt
from
    cb_resource_mf crmf
    join cb_resource_dm crd on
        crd.dw_resource_shk = crmf.dw_resource_shk
    join cb_service_type_dm cstd on
        cstd.dw_service_type_shk = crmf.dw_service_type_shk
    join date_dm dd on
        dd.dw_date_sid = crmf.dw_event_date_sid
group by
    1,2,3,4,5,6,7,8
order by
    1,2,3,4,5,6,7,8
;

-- Aggregation by month by account and be name
select
     crd.organization_name
    ,crd.be_name
    ,crd.account_name
    ,dd.cal_month_dt
    ,sum( crmf.compute_credit_cnt )      as compute_credit_cnt
    ,sum( crmf.compute_cost_amt )        as compute_cost_amt
    ,sum( crmf.storage_byte_cnt )        as storage_byte_cnt
    ,sum( crmf.storage_cost_amt )        as storage_cost_amt
    ,sum( crmf.data_xfer_byte_cnt )      as data_xfer_byte_cnt
    ,sum( crmf.data_xfer_cost_amt )      as data_xfer_cost_amt
from
    cb_resource_mf crmf
    join cb_resource_dm crd on
        crd.dw_resource_shk = crmf.dw_resource_shk
    join cb_service_type_dm cstd on
        cstd.dw_service_type_shk = crmf.dw_service_type_shk
    join date_dm dd on
        dd.dw_date_sid = crmf.dw_event_date_sid
group by
    1,2,3,4
order by
    1,2,3,4
;

-- Aggregation by month by account, reader_account and be name
select
     crd.organization_name
    ,crd.be_name
    ,crd.account_name
    ,crd.reader_account_name
    ,dd.cal_month_dt
    ,sum( crmf.compute_credit_cnt )      as compute_credit_cnt
    ,sum( crmf.compute_cost_amt )        as compute_cost_amt
    ,sum( crmf.storage_byte_cnt )        as storage_byte_cnt
    ,sum( crmf.storage_cost_amt )        as storage_cost_amt
    ,sum( crmf.data_xfer_byte_cnt )      as data_xfer_byte_cnt
    ,sum( crmf.data_xfer_cost_amt )      as data_xfer_cost_amt
from
    cb_resource_mf crmf
    join cb_resource_dm crd on
        crd.dw_resource_shk = crmf.dw_resource_shk
    join cb_service_type_dm cstd on
        cstd.dw_service_type_shk = crmf.dw_service_type_shk
    join date_dm dd on
        dd.dw_date_sid = crmf.dw_event_date_sid
group by
    1,2,3,4,5
order by
    1,2,3,4,5
;

-- Service Type by Account
select
     crd.organization_name
    ,crd.be_name
    ,crd.account_name
    ,cstd.service_type_name
    ,sum( crmf.compute_credit_cnt )      as compute_credit_cnt
    ,sum( crmf.compute_cost_amt )        as compute_cost_amt
    ,sum( crmf.storage_byte_cnt )        as storage_byte_cnt
    ,sum( crmf.storage_cost_amt )        as storage_cost_amt
    ,sum( crmf.data_xfer_byte_cnt )      as data_xfer_byte_cnt
    ,sum( crmf.data_xfer_cost_amt )      as data_xfer_cost_amt
from
    cb_resource_mf crmf
    join cb_resource_dm crd on
        crd.dw_resource_shk = crmf.dw_resource_shk
    join cb_service_type_dm cstd on
        cstd.dw_service_type_shk = crmf.dw_service_type_shk
    join date_dm dd on
        dd.dw_date_sid = crmf.dw_event_date_sid
group by
    1,2,3,4
order by
    1,2,3,4
;

select
     crd.organization_name
    ,crd.account_name
    ,crd.reader_account_name
    ,crd.resource_type_cd
    ,crd.resource_name
    ,cstd.service_type_name
    ,dd.cal_month_dt
    ,sum( crmf.compute_credit_cnt )      as compute_credit_cnt
    ,sum( crmf.compute_cost_amt )        as compute_cost_amt
    ,sum( crmf.storage_byte_cnt )        as storage_byte_cnt
    ,sum( crmf.storage_cost_amt )        as storage_cost_amt
    ,sum( crmf.data_xfer_byte_cnt )      as data_xfer_byte_cnt
    ,sum( crmf.data_xfer_cost_amt )      as data_xfer_cost_amt
from
    cb_resource_mf crmf
    join cb_resource_dm crd on
        crd.dw_resource_shk = crmf.dw_resource_shk
    join cb_service_type_dm cstd on
        cstd.dw_service_type_shk = crmf.dw_service_type_shk
    join date_dm dd on
        dd.dw_date_sid = crmf.dw_event_date_sid
group by
    1,2,3,4,5,6,7
order by
    1,2,3,4,5,6,7
;

-- Summary for just reader accounts
select
     crd.organization_name
    ,crd.account_name
    ,crd.reader_account_name
    ,crd.resource_type_cd
    ,crd.resource_name
    ,cstd.service_type_name
    ,dd.cal_month_dt
    ,sum( crmf.compute_credit_cnt )      as compute_credit_cnt
    ,sum( crmf.compute_cost_amt )        as compute_cost_amt
    ,sum( crmf.storage_byte_cnt )        as storage_byte_cnt
    ,sum( crmf.storage_cost_amt )        as storage_cost_amt
    ,sum( crmf.data_xfer_byte_cnt )      as data_xfer_byte_cnt
    ,sum( crmf.data_xfer_cost_amt )      as data_xfer_cost_amt
from
    cb_resource_mf crmf
    join cb_resource_dm crd on
        crd.dw_resource_shk = crmf.dw_resource_shk
    join cb_service_type_dm cstd on
        cstd.dw_service_type_shk = crmf.dw_service_type_shk
    join date_dm dd on
        dd.dw_date_sid = crmf.dw_event_date_sid
where
    crd.reader_account_name != '~'
group by
    1,2,3,4,5,6,7
order by
    1,2,3,4,5,6,7
;
