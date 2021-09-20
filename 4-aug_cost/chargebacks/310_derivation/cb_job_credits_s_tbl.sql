--------------------------------------------------------------------
--  Purpose: create psa tables
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------

--
-- permanent table with retention days
--
create table if not exists &{l_il_db}.&{l_il_schema}.cb_job_credits_s
(
     dw_query_history_shk           binary( 20 )        not null
    ,start_time                     timestamp_ltz       not null
    ,hour_seq_no                    number              not null
    --
    ,organization_name              varchar( 250 )      not null
    ,account_name                   varchar( 250 )      not NULL
    ,reader_account_name              varchar( 250 )      NOT null
    ,region_name                    varchar( 250 )      not null
    ,warehouse_name                 text
    ,job_credits_used_compute       float
    --
    ,job_hour_cnt                   number
    ,credits_used_compute           float
    ,period_duration_pct            float
    ,period_duration                float
    ,adj_period_duration            float
    ,credits_consumed_bt            number
    ,job_period_start_ts            timestamp_ltz
    ,period_start_ts                timestamp_ltz
    ,period_end_ts                  timestamp_ltz
    ,adj_job_start_ts               timestamp_ltz
    --
    ,dw_load_ts                     timestamp_ltz       not null
)
data_retention_time_in_days = 1
;
