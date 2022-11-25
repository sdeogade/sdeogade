--------------------------------------------------------------------
--  Purpose: create psa tables
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------

create transient table if not exists &{l_common_db}.&{l_common_schema}.dw_delta_date
(
     event_dt       timestamp_ltz not null
    ,dw_load_ts     timestamp_ltz not null
)
data_retention_time_in_days = 0
;


