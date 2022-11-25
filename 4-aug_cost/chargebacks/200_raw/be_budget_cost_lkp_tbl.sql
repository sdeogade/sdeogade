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
create table if not exists &{l_raw_db}.&{l_raw_schema}.be_budget_cost_lkp
(
     be_name                        varchar( 250 )      not NULL
    ,month_begin_dt                 date                NOT null
    ,compute_credit_cnt             float               not null
    --
    ,dw_load_ts                     timestamp_ltz       not null
)
data_retention_time_in_days = 1
;
