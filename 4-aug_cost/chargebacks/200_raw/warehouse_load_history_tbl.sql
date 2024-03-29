--------------------------------------------------------------------
--  Purpose: create psa tables
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------

--
-- transient staging table with no retention days
--
create transient table if not exists &{l_raw_db}.&{l_raw_schema}.warehouse_load_history_stg
(
     organization_name              varchar( 250 )      not null
    ,account_name                   varchar( 250 )      not null
    ,region_name                    varchar( 250 )      not null
    ,start_time                     timestamp_ltz       not null
    ,end_time                       timestamp_ltz       not null
    ,warehouse_id                   number              not null
    ,warehouse_name                 varchar( 250 )      not null
    ,avg_running                    float               not null
    ,avg_queued_load                float               not null
    ,avg_queued_provisioning        float               not null
    ,avg_blocked                    float               not null
    --
    ,dw_file_name                   varchar( 250 )      not null
    ,dw_file_row_no                 number              not null
    ,dw_load_ts                     timestamp_ltz       not null
)
data_retention_time_in_days = 0
;

--
-- permanent history table with retention days
--
create table if not exists &{l_raw_db}.&{l_raw_schema}.warehouse_load_history
(
     dw_warehouse_load_history_shk                   binary( 20 )        not null
    --
    ,organization_name              varchar( 250 )      not null
    ,account_name                   varchar( 250 )      not null
    ,region_name                    varchar( 250 )      not null
    ,start_time                     timestamp_ltz       not null
    ,end_time                       timestamp_ltz       not null
    ,warehouse_id                   number              not null
    ,warehouse_name                 varchar( 250 )      not null
    ,avg_running                    float               not null
    ,avg_queued_load                float               not null
    ,avg_queued_provisioning        float               not null
    ,avg_blocked                    float               not null
    --
    ,dw_file_name                   varchar( 250 )      not null
    ,dw_file_row_no                 number              not null
    ,dw_load_ts                     timestamp_ltz       not null
)
data_retention_time_in_days = 1
;
