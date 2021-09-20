--------------------------------------------------------------------
--  Purpose: load into stage table
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------

truncate table &{l_raw_db}.&{l_raw_schema}.data_transfer_history_stg;

copy into
    &{l_raw_db}.&{l_raw_schema}.data_transfer_history_stg
from
    (
    select
         s.$1                                            -- organization_name
        ,s.$2                                            -- account_name
        ,s.$3                                            -- region_name
        ,s.$4                                            -- start_time
        ,s.$5                                            -- end_time
        ,s.$6                                            -- source_cloud
        ,s.$7                                            -- source_region
        ,s.$8                                            -- target_cloud
        ,s.$9                                            -- target_region
        ,s.$10                                           -- bytes_transferred
        ,s.$11                                           -- transfer_type
        ,metadata$filename                               -- dw_file_name
        ,metadata$file_row_number                        -- dw_file_row_no
        ,current_timestamp()                             -- dw_load_ts
    from
        @&{l_common_db}.&{l_common_schema}.account_usage_stg/data_transfer_history s
    )
file_format = ( type=csv field_optionally_enclosed_by = '"' )
purge = true
;
