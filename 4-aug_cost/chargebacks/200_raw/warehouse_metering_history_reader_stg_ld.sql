--------------------------------------------------------------------
--  Purpose: load into stage table
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------

truncate table &{l_raw_db}.&{l_raw_schema}.warehouse_metering_history_reader_stg;

copy into
    &{l_raw_db}.&{l_raw_schema}.warehouse_metering_history_reader_stg
from
    (
    select
         s.$1                                            -- organization_name
        ,s.$2                                            -- account_name
        ,s.$3                                            -- region_name
        ,s.$4                                            -- reader_account_name
        ,s.$5                                            -- start_time
        ,s.$6                                            -- end_time
        ,s.$7                                            -- warehouse_id
        ,s.$8                                            -- warehouse_name
        ,s.$9                                            -- credits_used
        ,s.$10                                           -- credits_used_compute
        ,s.$11                                           -- credits_used_cloud_services
        ,s.$12                                           -- reader_account_deleted_on
        ,metadata$filename                               -- dw_file_name
        ,metadata$file_row_number                        -- dw_file_row_no
        ,current_timestamp()                             -- dw_load_ts
    from
        @&{l_common_db}.&{l_common_schema}.account_usage_stg/reader_warehouse_metering_history s
    )
file_format = ( type=csv field_optionally_enclosed_by = '"' )
purge = true
;
