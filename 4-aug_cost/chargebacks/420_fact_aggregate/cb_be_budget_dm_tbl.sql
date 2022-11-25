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

create table if not exists &{l_pl_db}.&{l_pl_schema}.cb_be_budget_dm 

(
     dw_be_shk                      binary( 20 )
	,be_name						varchar
	 
    ,dw_event_date_sid              number
    --
    ,event_month_dt                 date
    ,bgt_compute_credit_cnt         float
    --
    ,dw_load_ts                     timestamp_ltz       not null
    ,dw_update_ts                   timestamp_ltz       not null
)
data_retention_time_in_days = 1
;
