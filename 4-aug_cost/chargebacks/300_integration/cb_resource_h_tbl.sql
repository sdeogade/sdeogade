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
create table if not exists &{l_il_db}.&{l_il_schema}.cb_resource_h
(
     dw_resource_shk                binary( 20 )        not null
    --
    ,organization_name              varchar( 250 )      not null
    ,region_name                    varchar( 250 )      not null
    ,account_name                   varchar( 250 )      not NULL
    ,reader_account_name            varchar( 250 )      NOT null
    ,resource_name                  varchar( 250 )      not null
    ,resource_type_cd               varchar( 50 )       not null
    --
    ,dw_load_ts                     timestamp_ltz       not null
    ,dw_update_ts                   timestamp_ltz       not null
)
data_retention_time_in_days = 1
;
