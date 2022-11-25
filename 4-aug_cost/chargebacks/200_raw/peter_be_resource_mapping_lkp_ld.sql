

insert overwrite into  &{l_raw_db}.&{l_raw_schema}.be_resource_mapping_lkp
with l_mapping as
(
  select to_char( null ) as match_pattern, to_number( null ) as priority_no, to_variant( null ) as tag_json
    --- LDE
    union all select '(dev|tst|prd)_plan_lde.*',     1, object_construct( 'be','plan','env','dev','team','lde','gl_acct','xxxxx.xxxxx' )
    union all select 'plan_lde.*',                   1, object_construct( 'be','plan','env','dev','team','lde','gl_acct','xxxxx.xxxxx' )
    union all select 'lde.*',                        1, object_construct( 'be','plan','env','dev','team','lde','gl_acct','xxxxx.xxxxx' )
    union all select 'ml_medium_coreload',           1, object_construct( 'be','plan','env','dev','team','lde','gl_acct','xxxxx.xxxxx' )
    union all select 'jens',                         1, object_construct( 'be','plan','env','dev','team','lde','gl_acct','xxxxx.xxxxx' )
  
    --- LPDM
    union all select '(dev|tst|prd)_plat_lpdm.*',    1, object_construct( 'be','plan','env','dev','team','lpdm','gl_acct','xxxxx.xxxxx' )
    union all select 'platform_.*',                  1, object_construct( 'be','plat','env','dev','team','lpdm','gl_acct','xxxxx.xxxxx' )
    union all select 'compute_wh',                   1, object_construct( 'be','plat','env','dev','team','lpdm','gl_acct','xxxxx.xxxxx' )
    union all select 'cloud_services_only',          1, object_construct( 'be','plan','env','dev','team','lpdm','gl_acct','xxxxx.xxxxx' )
    union all select 'plat_lpdm.*',                  1, object_construct( 'be','plan','env','dev','team','lpdm','gl_acct','xxxxx.xxxxx' )
    --- WMS
     union all select '(dev|tst|prd)_exec_wms.*',     1, object_construct( 'be','exec','env','dev','team','wms','gl_acct','xxxxx.xxxxx' )
 
    --- LCT
    union all select '(dev|tst|prd)_plat_lct.*',     1, object_construct( 'be','plat','env','dev','team','lct','gl_acct','xxxxx.xxxxx' )
    union all select 'LCT_ANALYTICS_WH',             1, object_construct( 'be','plat','env','dev','team','lct','gl_acct','xxxxx.xxxxx' )
  
    --- Catch ALL
    union all select '.*',                           5, object_construct( 'be','?','env','dev','team','?','gl_acct','xxxxx.xxxxx' )
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
