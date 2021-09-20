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
create table if not exists &{l_raw_db}.&{l_raw_schema}.be_resource_mapping_lkp
(
     match_pattern                  varchar( 100 )
    ,priority_no                    number
    ,tag_json                       variant
    --
    ,dw_load_ts                     timestamp_ltz       not null
)
data_retention_time_in_days = 1
;
