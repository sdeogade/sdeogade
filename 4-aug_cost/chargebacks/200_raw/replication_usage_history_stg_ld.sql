--------------------------------------------------------------------
--  Purpose: load into stage table
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------

truncate table &{l_raw_db}.&{l_raw_schema}.replication_usage_history_stg;

copy into
    &{l_raw_db}.&{l_raw_schema}.replication_usage_history_stg
from
    (
    select
         s.$1                                            -- organization_name
        ,s.$2                                            -- account_name
        ,s.$3                                            -- region_name
        ,s.$4                                            -- start_time
        ,s.$5                                            -- end_time
        ,s.$6                                            -- database_id
        ,s.$7                                            -- database_name
        ,s.$8                                            -- credits_used
        ,s.$9                                            -- bytes_transferred
        ,metadata$filename                               -- dw_file_name
        ,metadata$file_row_number                        -- dw_file_row_no
        ,current_timestamp()                             -- dw_load_ts
    from
        @&{l_common_db}.&{l_common_schema}.account_usage_stg/replication_usage_history s
    )
file_format = ( type=csv field_optionally_enclosed_by = '"' )
purge = true
;
