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
    &{l_pl_db}.&{l_pl_schema}.cb_resource_dm
select
     brms.dw_resource_shk
    --

    ,brms.be_name           
    ,crh.organization_name 
    ,crh.account_name    
    ,crh.reader_account_name
    ,crh.region_name
    ,crh.resource_name
    ,crh.resource_type_cd
    ,brms.tag_json:be::string     as business_entity
    ,brms.tag_json:team::string     as team_name
    ,brms.tag_json:product_group::string     as product_group
    ,brms.tag_json:environment_type::string      as environment_type
    ,brms.tag_json:cost_center_id::string  as cost_center_id
    ,brms.tag_json:customer::string  as customer
  
    ,current_timestamp()            as dw_load_ts
from
    &{l_il_db}.&{l_il_schema}.be_resource_mapping_s brms
    join &{l_il_db}.&{l_il_schema}.cb_resource_h crh on
        crh.dw_resource_shk = brms.dw_resource_shk
order by
    2,3,4,5
;
