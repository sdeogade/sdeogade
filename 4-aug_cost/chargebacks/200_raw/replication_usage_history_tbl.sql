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
create transient table if not exists &{l_raw_db}.&{l_raw_schema}.replication_usage_history_stg
(
     organization_name              varchar( 250 )      null
    ,account_name                   varchar( 250 )      null
    ,region_name                    varchar( 250 )      null
    ,start_time                     timestamp_ltz       null
    ,end_time                       timestamp_ltz       null
    ,database_id                    number              null
    ,database_name                  varchar( 250 )      null
    ,credits_used                   float               null
    ,bytes_transferred              float               null
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
create table if not exists &{l_raw_db}.&{l_raw_schema}.replication_usage_history
(
     dw_replication_usage_history_shk                   binary( 20 )        null
    --
    ,organization_name              varchar( 250 )      null
    ,account_name                   varchar( 250 )      null
    ,region_name                    varchar( 250 )      null
    ,start_time                     timestamp_ltz       null
    ,end_time                       timestamp_ltz       null
    ,database_id                    number              null
    ,database_name                  varchar( 250 )      null
    ,credits_used                   float               null
    ,bytes_transferred              float               null
    --
    ,dw_file_name                   varchar( 250 )      not null
    ,dw_file_row_no                 number              not null
    ,dw_load_ts                     timestamp_ltz       not null
)
data_retention_time_in_days = 1
;
