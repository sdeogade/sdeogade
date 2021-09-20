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
create table if not exists &{l_il_db}.&{l_il_schema}.cb_be_h
(
     dw_be_shk                binary( 20 )        not null
    --
    ,be_name                  varchar( 250 )      not null
    --
    ,dw_load_ts               timestamp_ltz       not null
    ,dw_update_ts             timestamp_ltz       not null
)
data_retention_time_in_days = 1
;
