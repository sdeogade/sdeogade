--------------------------------------------------------------------
--  Purpose:
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------

--------------------------------------------------------------------
-- load delta
--
insert into
    &{l_il_db}.&{l_il_schema}.cb_account_resource_l
select
     cah.dw_account_shk
    ,crh.dw_resource_shk
    ,current_timestamp()            as dw_load_ts
    ,current_timestamp()            as dw_update_ts
from
     &{l_il_db}.&{l_il_schema}.cb_resource_h crh
     join &{l_il_db}.&{l_il_schema}.cb_account_h cah on
             cah.organization_name   = crh.organization_name
         and cah.region_name         = crh.region_name
         and cah.account_name        = crh.account_name
         AND cah.reader_account_name = crh.reader_account_name
where
    (cah.dw_account_shk, crh.dw_resource_shk) not in
    (
        select dw_account_shk, dw_resource_shk from &{l_il_db}.&{l_il_schema}.cb_account_resource_l
    )
order by
    1,2
;
