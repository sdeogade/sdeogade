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
create table if not exists &{l_pl_db}.&{l_pl_schema}.cb_service_type_dm
(
     dw_service_type_shk            binary( 20 )        not null
    ,dw_hash_diff                   binary( 20 )        not null
    ,service_type_cd                varchar( 50 )       not null
    ,service_type_name              varchar( 250 )      not null
    ,service_type_group_name        varchar( 250 )      not null
    ,active_dt                      date                not null
    ,inactive_dt                    date                not null
    --
    ,dw_load_ts                     timestamp_ltz       not null
    ,dw_update_ts                   timestamp_ltz       not null
)
data_retention_time_in_days = 1
;
