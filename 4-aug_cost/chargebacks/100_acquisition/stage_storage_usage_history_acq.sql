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
        ifnull( dateadd( hour, -4, max( usage_date ) ), dateadd( month, -13, current_timestamp() ) ) as last_control_dt
    from
        &{l_raw_db}.&{l_raw_schema}.stage_storage_usage_history
    where
            organization_name = upper( '&{l_org_name}' )
        and account_name      = upper( '&{l_account_name}' )
);
*/
--Suraj : removed comment

--------------------------------------------------------------------
-- !!! Note: code to use when demonstrating across multiple accounts.
--           uses a fixed delta window.


set l_last_control_dt =
(
    select dateadd( hour, -&{l_delta_hours}, current_timestamp() ) as last_control_dt
);

copy into
    @&{l_common_db}.&{l_common_schema}.account_usage_stg/stage_storage_usage_history/&{l_account_name}
from
(
    select
         upper( '&{l_org_name}' )           as organization_name
        ,current_account()                  as account_name
        ,current_region()                   as region_name
        ,s.usage_date
        ,s.average_stage_bytes
    from
        snowflake.account_usage.stage_storage_usage_history s
    where
        s.usage_date >= to_timestamp( $l_last_control_dt )
)
file_format      = ( type=csv field_optionally_enclosed_by = '"' )
include_query_id = true
overwrite        = false
single           = false
;

list @&{l_common_db}.&{l_common_schema}.account_usage_stg;
