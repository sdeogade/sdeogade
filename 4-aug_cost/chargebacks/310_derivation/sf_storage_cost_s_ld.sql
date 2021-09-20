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
    &{l_il_db}.&{l_il_schema}.sf_storage_cost_s
select
     cah.dw_account_shk
    ,cst.active_dt
    --
    ,cst.cost_amt
    ,cst.inactive_dt
    --
    ,current_timestamp()        as dw_load_ts
from
    &{l_raw_db}.&{l_raw_schema}.sf_storage_cost_lkp cst
    join &{l_il_db}.&{l_il_schema}.cb_account_h cah on
        cah.organization_name = cst.organization_name
		and cah.account_name  = cst.account_name
        AND cah.region_name   = cst.region_name
order by
    2,1
;