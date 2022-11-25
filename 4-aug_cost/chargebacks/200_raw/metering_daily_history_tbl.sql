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
create transient table if not exists &{l_raw_db}.&{l_raw_schema}.metering_daily_history_stg
(
     organization_name                  varchar( 250 )      not null
    ,account_name                       varchar( 250 )      not null
    ,region_name                        varchar( 250 )      not null
    ,service_type                       varchar( 250 )      not null
    ,usage_date                         timestamp_ltz       not null
    ,credits_used_compute               number              not null
    ,credits_used_cloud_services        number              not null
    ,credits_used                       number              not null
    ,credits_adjustment_cloud_services  number              not null
    ,credits_billed                     number              not null
    --
    ,dw_file_name                       varchar( 250 )      not null
    ,dw_file_row_no                     number              not null
    ,dw_load_ts                         timestamp_ltz       not null
)
data_retention_time_in_days = 0
;

--
-- permanent history table with retention days
--
create table if not exists &{l_raw_db}.&{l_raw_schema}.metering_daily_history
(
     dw_metering_daily_history_shk                       binary( 20 )        not null
    --
    ,organization_name                  varchar( 250 )      not null
    ,account_name                       varchar( 250 )      not null
    ,region_name                        varchar( 250 )      not null
    ,service_type                       varchar( 250 )      not null
    ,usage_date                         timestamp_ltz       not null
    ,credits_used_compute               number              not null
    ,credits_used_cloud_services        number              not null
    ,credits_used                       number              not null
    ,credits_adjustment_cloud_services  number              not null
    ,credits_billed                     number              not null
    --
    ,dw_file_name                       varchar( 250 )      not null
    ,dw_file_row_no                     number              not null
    ,dw_load_ts                         timestamp_ltz       not null
)
data_retention_time_in_days = 1
;
