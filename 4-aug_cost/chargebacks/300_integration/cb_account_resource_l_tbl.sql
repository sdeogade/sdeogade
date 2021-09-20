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
create table if not exists &{l_il_db}.&{l_il_schema}.cb_account_resource_l
(
     dw_account_shk                 binary( 20 )        not null
    ,dw_resource_shk                binary( 20 )        not null
    --
    ,dw_load_ts                     timestamp_ltz       not null
    ,dw_update_ts                   timestamp_ltz       not null
)
data_retention_time_in_days = 1
;
