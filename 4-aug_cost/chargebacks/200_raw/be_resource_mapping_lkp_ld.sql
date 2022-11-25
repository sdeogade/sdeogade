--------------------------------------------------------------------
--  Purpose: load lookup table with an insert-only pattern since the
--           pk has the potential of changing.
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
-- mapping match patterns and priorities

insert overwrite into
--
-- For all product development it should be 474000 with the exception of
-- IS development which is an addback so the Cost Center ID for them is 402100
-- once we start creating customer POC or demo environments the cost
-- center ID will either be 206500 or 131000 depending on whether
-- or not they are paid customer engagements

-- https://jda365.sharepoint.com/:x:/t/ITG/team-BI/eds/EYctzU9oQR1DivMk-ONKMVQB0-Qb9sFvYR-ZtOTzjcz0zw?e=coVEHa

    &{l_raw_db}.&{l_raw_schema}.be_resource_mapping_lkp
with l_mapping as
(
    -- 0 exact match
    -- 1 team match (e.g., ent_prd_[^_]_team_.*)
    -- 2 general business entity match
    -- 5 match all to catch and categorize new patterns
    select to_char( null ) as match_pattern, to_number( null ) as priority_no, to_variant( null ) as tag_json


    --- LDE
    union all select '(dev|tst|prd|uat)_plan_lde_ui.*',     1, object_construct( 'product_group','Luminate Demand Edge', 'be','plan_lde','environment_type','dev','team', 'lde_ui','cost_center_id','474000', 'Customer', 'Unassigned' )
    union all select '(dev|tst|prd|uat)_plan_lde.*',        2, object_construct( 'product_group','Luminate Demand Edge', 'be','plan_lde','environment_type','dev','team', 'lde','cost_center_id','474000', 'Customer', 'Unassigned' )
    union all select 'plan_lde.*',                          2, object_construct( 'product_group','Luminate Demand Edge', 'be','plan_lde','environment_type','dev','team', 'lde','cost_center_id','474000', 'Customer', 'Unassigned' )
    union all select 'lde.*',                               2, object_construct( 'product_group','Luminate Demand Edge', 'be','plan_lde','environment_type','dev','team', 'lde','cost_center_id','474000', 'Customer', 'Unassigned' )
    union all select 'ml_medium_coreload',                  0, object_construct( 'product_group','Luminate Demand Edge', 'be','plan_lde','environment_type','dev','team', 'lde','cost_center_id','474000', 'Customer', 'Unassigned' )
    union all select 'jens',                                0, object_construct( 'product_group','Luminate Demand Edge', 'be','plan_lde','environment_type','dev','team', 'lde','cost_center_id','474000', 'Customer', 'Unassigned' )

    --- LPDM
    union all select '(dev|tst|prd|uat)_plat_lpdm.*',   1, object_construct( 'product_group','Luminate Platform', 'be','plat_lpdm','environment_type','dev','team','lpdm','cost_center_id','474000', 'Customer', 'Unassigned' )

    union all select 'platform_.*',                     1, object_construct( 'product_group','Luminate Platform', 'be','plat_lpdm','environment_type','dev','team','lpdm','cost_center_id','474000', 'Customer', 'Unassigned' )
    union all select 'plat_lpdm.*',                     1, object_construct( 'product_group','Luminate Platform', 'be','plat_lpdm','environment_type','dev','team','lpdm','cost_center_id','474000', 'Customer', 'Unassigned' )
    union all select 'compute_wh',                      0, object_construct( 'product_group','Luminate Platform', 'be','plat_lpdm','environment_type','dev','team','lpdm','cost_center_id','474000', 'Customer', 'Unassigned' )
    union all select 'temp_wh',                         0, object_construct( 'product_group','Luminate Platform', 'be','plat_lpdm','environment_type','dev','team','lpdm','cost_center_id','474000', 'Customer', 'Unassigned' )
    union all select 'psr_wh',                          0, object_construct( 'product_group','Luminate Platform', 'be','plat_lpdm','environment_type','dev','team','lpdm','cost_center_id','474000', 'Customer', 'Unassigned' )
    union all select 'cloud_services_only',             0, object_construct( 'product_group','Luminate Platform', 'be','plat_lpdm','environment_type','dev','team','lpdm','cost_center_id','474000', 'Customer', 'Unassigned' )

    --- WMS

     union all select '(dev|tst|prd|uat)_exec_wms.*',     1, object_construct( 'product_group','WMS', 'be','exec_wms','environment_type','dev','team','wms','cost_center_id','474000', 'Customer', 'Unassigned' )

    --- LCT
    union all select '(dev|tst|prd|uat)_plat_lct.*',     1, object_construct( 'product_group','Luminate Control Tower', 'be','plat_lct','environment_type','dev','team','lct','cost_center_id','474000', 'Customer', 'Unassigned' )
    union all select 'LCT_ANALYTICS_WH',                 0, object_construct( 'product_group','Luminate Control Tower', 'be','plat_lct','environment_type','dev','team','lct','cost_center_id','474000', 'Customer', 'Unassigned' )

    --- LVDS
    union all select '(dev|tst|prd|uat)_plat_lvds.*',     1, object_construct( 'product_group','Luminate Platform', 'be','plat_lvds','environment_type','dev','team','lvds','cost_center_id','474000', 'Customer', 'Unassigned' )

    --- LUI
    union all select '(dev|tst|prd|uat)_plat_lui.*',     1, object_construct( 'product_group','Luminate Platform', 'be','plat_lui','environment_type','dev','team','lui','cost_center_id','474000', 'Customer', 'Unassigned' )

    --- RETAIL
    union all select '(dev|tst|prd|uat)_plan_retail.*',     1, object_construct( 'product_group','Luminate Platform', 'be','plan_retail','environment_type','dev','team','retail','cost_center_id','474000', 'Customer', 'Unassigned' )


    
    --- IS / DMS

    union all select '(dev|tst|prd|uat)_is_dms.*',     1, object_construct( 'product_group', 'Data Management and Analytics', 'be','is_dms','environment_type','dev','team','dms','cost_center_id','402100' )



    --- Default Catch all
    union all select '.*',                           5, object_construct( 'product_group','?', 'be','?','environment_type','dev','team','?','cost_center_id','?', 'Customer', 'Unassigned' )
    --
)
select
     lm.match_pattern
    ,lm.priority_no
    ,lm.tag_json
    ,current_timestamp()    as dw_load_ts
from
    l_mapping lm
where
    lm.tag_json is not null
;
