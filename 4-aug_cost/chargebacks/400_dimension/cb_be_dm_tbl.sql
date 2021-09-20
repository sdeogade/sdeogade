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
create table if not exists &{l_pl_db}.&{l_pl_schema}.cb_be_dm
(
     dw_be_shk                 binary( 20 )
    --
    ,be_name              varchar( 250 )
       --
    ,dw_load_ts                     timestamp_ltz       not null
)
data_retention_time_in_days = 1
;
