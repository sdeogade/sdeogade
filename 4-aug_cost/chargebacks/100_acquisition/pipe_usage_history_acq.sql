--------------------------------------------------------------------
--  Purpose: data acquisition
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------

--------------------------------------------------------------------
-- !!! Note: this first step is an example of what would need to be
--           executed against the target account to get the last
--           control date high water mark.  When implemented this
--           would be executed with a wrapper script like python and
--           then the date could be used to execute the copy into step
--           against the source account.
--
-- pull last event date from the table and back up n hours for late
-- arriving data.
--
/*
set l_last_control_dt =
(
    select
        ifnull( dateadd( hour, -4, max( start_time ) ), dateadd( month, -13, current_timestamp() ) ) as last_control_dt
    from
        &{l_raw_db}.&{l_raw_schema}.pipe_usage_history
    where
            organization_name = upper( '&{l_org_name}' )
        and account_name      = upper( '&{l_account_name}' )
);
*/


--------------------------------------------------------------------
-- !!! Note: code to use when demonstrating across multiple accounts.
--           uses a fixed delta window.

set l_last_control_dt =
(
    select dateadd( hour, -&{l_delta_hours}, current_timestamp() ) as last_control_dt
);

copy into
    @&{l_common_db}.&{l_common_schema}.account_usage_stg/pipe_usage_history/&{l_account_name}
from
(
    select
         upper( '&{l_org_name}' )           as organization_name
        ,current_account()                  as account_name
        ,current_region()                   as region_name
        ,s.start_time
        ,s.end_time
        ,s.credits_used
        ,s.bytes_inserted
        ,s.files_inserted
        ,s.pipe_id
        ,s.pipe_name
        --
        ,p.pipe_schema             as schema_name
        ,p.pipe_catalog            as database_name
        --
    from
        snowflake.account_usage.pipe_usage_history s
        left join snowflake.account_usage.pipes p on
            p.pipe_id = s.pipe_id
    where
        s.start_time >= to_timestamp( $l_last_control_dt )
)
file_format      = ( type=csv field_optionally_enclosed_by = '"' )
include_query_id = true
overwrite        = false
single           = false
;

list @&{l_common_db}.&{l_common_schema}.account_usage_stg;
