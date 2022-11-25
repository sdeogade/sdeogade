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
create table if not exists &{l_pl_db}.&{l_pl_schema}.cb_account_df
(
     dw_account_shk                 binary( 20 )
    ,dw_event_date_sid              number
    --
    ,event_dt                       date
    ,compute_credit_cnt             float
    ,compute_cost_amt               float
    ,storage_byte_cnt               number
    ,storage_cost_amt               float
    ,data_xfer_byte_cnt             number
    ,data_xfer_cost_amt             float
    ,total_cost_amt                 float
    --
    ,dw_load_ts                     timestamp_ltz       not null
    ,dw_update_ts                   timestamp_ltz       not null
)
data_retention_time_in_days = 1
;
