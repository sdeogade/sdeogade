--------------------------------------------------------------------
--  Purpose: load into stage table
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------

truncate table &{l_raw_db}.&{l_raw_schema}.metering_daily_history_stg;

copy into
    &{l_raw_db}.&{l_raw_schema}.metering_daily_history_stg
from
    (
    select
         s.$1                                            -- organization_name
        ,s.$2                                            -- account_name
        ,s.$3                                            -- region_name
        ,s.$4                                            -- service_type
        ,s.$5                                            -- usage_date
        ,s.$6                                            -- credits_used_compute
        ,s.$7                                            -- credits_used_cloud_services
        ,s.$8                                            -- credits_used
        ,s.$9                                            -- credits_adjustment_cloud_services
        ,s.$10                                           -- credits_billed
        ,metadata$filename                               -- dw_file_name
        ,metadata$file_row_number                        -- dw_file_row_no
        ,current_timestamp()                             -- dw_load_ts
    from
        @&{l_common_db}.&{l_common_schema}.account_usage_stg/metering_daily_history s
    )
file_format = ( type=csv field_optionally_enclosed_by = '"' )
purge = true
;
