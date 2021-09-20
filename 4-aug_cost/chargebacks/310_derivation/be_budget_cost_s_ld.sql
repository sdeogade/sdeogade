--------------------------------------------------------------------
--  Purpose: load lookup table with an insert-only pattern since the
--           pk has the potential of changing.
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------

--------------------------------------------------------------------
-- full load
--
-- mappings can change which will and 
-- should remove old mappings that may have been incorrect, therefore
-- this needs to be a full load each time.
--
insert overwrite into 
    &{l_il_db}.&{l_il_schema}.be_budget_cost_s
select
     cah.dw_be_shk
    ,cst.be_name
    ,&{l_common_db}.&{l_common_schema}.date_sid_f( date_trunc( month, cst.month_begin_dt ) )    as dw_event_date_sid
    ,cst.month_begin_dt
    --
    ,cst.compute_credit_cnt
    ,current_timestamp()        as dw_load_ts
from
    &{l_raw_db}.&{l_raw_schema}.be_budget_cost_lkp cst
    join &{l_il_db}.&{l_il_schema}.cb_be_h cah on
        cah.be_name = cst.be_name
order by
    2, 1
;
