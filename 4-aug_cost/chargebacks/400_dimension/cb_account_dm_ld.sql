--------------------------------------------------------------------
--  Purpose: 
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------

--------------------------------------------------------------------
-- a period-based delta scenario is not currently applicable to this
-- load process. a full load is performed since combinations of 
-- pk values may no longer exist after changes to the source mapping
--

insert overwrite into 
    &{l_pl_db}.&{l_pl_schema}.cb_account_dm
select
     cah.dw_account_shk
    --
    ,cah.organization_name 
    ,cah.account_name    
    ,cah.reader_account_name
    ,cah.region_name
    --
    ,current_timestamp()            as dw_load_ts
from
    &{l_il_db}.&{l_il_schema}.cb_account_h cah
order by
    2,3,4
;
