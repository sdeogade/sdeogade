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
create table if not exists &{l_pl_db}.&{l_pl_schema}.cb_resource_dm
(
     dw_resource_shk                binary( 20 )
    --
    ,be_name                        varchar( 250 )
    ,organization_name              varchar( 250 )
    ,account_name                   varchar( 250 )
    ,reader_account_name            varchar( 250 )
    ,region_name                    varchar( 250 )
    ,resource_name                  varchar( 250 )
    ,resource_type_cd               varchar( 250 )
    ,business_entity                varchar( 250 )
    ,team                           varchar( 250 )
    ,product_group                  varchar( 250 )
    ,environment_type               varchar( 250 )
    ,cost_center_id                 varchar( 250 )
    ,customer                       varchar( 250 )
    
    ,dw_load_ts                     timestamp_ltz       not null
)
data_retention_time_in_days = 1
;
