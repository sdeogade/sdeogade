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
create table if not exists &{l_il_db}.&{l_il_schema}.be_resource_mapping_s
(
     dw_resource_shk                binary( 20 )        not null
    ,be_name                        varchar( 250 )      not null
    ,tag_json                       variant
    --
    ,priority_no                    number
    ,match_pattern                  varchar( 100 )
    --
    ,dw_load_ts                     timestamp_ltz       not null
)
data_retention_time_in_days = 1
;
