--------------------------------------------------------------------
--  Purpose:
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------

--------------------------------------------------------------------
-- pull staged delta date range
--
set (l_start_dt, l_end_dt ) = (select start_dt, dateadd( day, 1, end_dt ) from table( &{l_common_db}.&{l_common_schema}.dw_delta_date_range_f( 'all' ) ));

--------------------------------------------------------------------
-- load delta
--
insert into
    &{l_il_db}.&{l_il_schema}.cb_be_h
with l_be as
(
    select
        distinct brml.tag_json:be::string        as be_name
    from
        &{l_raw_db}.&{l_raw_schema}.be_resource_mapping_lkp brml 
)
,l_be_shk as
(
    select
        -- generate hash key
         sha1_binary( concat( lr.be_name ) )                   as dw_be_shk
        --
        ,lr.be_name
    from
        l_be lr
)
select
     -- generate hash key
     sha1_binary( concat( lrs.be_name ) )   as dw_be_shk
    --
    ,lrs.be_name
    --
    ,current_timestamp()            as dw_load_ts
    ,current_timestamp()            as dw_update_ts
from
    l_be_shk lrs
where
    lrs.dw_be_shk not in
    (
        select dw_be_shk from &{l_il_db}.&{l_il_schema}.cb_be_h
    )
order by
    2
;
