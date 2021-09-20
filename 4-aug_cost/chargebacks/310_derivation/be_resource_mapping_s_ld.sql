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
-- mapping match patterns and priorities can change which will and
-- should remove old mapping that may have been incorrect, therefore
-- this needs to be a full load each time.
--
insert overwrite into
    &{l_il_db}.&{l_il_schema}.be_resource_mapping_s
with l_mapping as
(
    select
        row_number() over( partition by crh.dw_resource_shk order by brml.priority_no ) as seq_no
       ,crh.dw_resource_shk
       ,brml.tag_json:be::string        as be_name
       ,brml.priority_no
       ,brml.match_pattern
       ,brml.tag_json
    from
        &{l_il_db}.&{l_il_schema}.cb_resource_h crh
        join &{l_raw_db}.&{l_raw_schema}.be_resource_mapping_lkp brml on
            regexp_like( crh.resource_name, brml.match_pattern, 'i' )
)
select
     lm.dw_resource_shk
    ,lm.be_name
    ,lm.tag_json
    --
    ,lm.priority_no
    ,lm.match_pattern
    --
    ,current_timestamp()        as dw_load_ts
from
    l_mapping lm
where
    lm.seq_no = 1 -- highest priority match
order by
    2,1
;
