--------------------------------------------------------------------
--  Purpose:
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
insert overwrite into &{l_common_db}.&{l_common_schema}.dw_delta_date
with l_delta_date as
(
    select distinct
        date_trunc( day, start_time )    as event_dt
    from
        &{l_raw_db}.&{l_raw_schema}.warehouse_metering_history_stg
    UNION
    select distinct
        date_trunc( day, start_time )    as event_dt
    from
        &{l_raw_db}.&{l_raw_schema}.warehouse_metering_history_reader_stg
    union
    select distinct
        date_trunc( day, usage_date )  as event_dt
    from
        &{l_raw_db}.&{l_raw_schema}.database_storage_usage_history_stg
    union
    select distinct
        date_trunc( day, usage_date )  as event_dt
    from
        &{l_raw_db}.&{l_raw_schema}.stage_storage_usage_history_stg
    union
    select distinct
        date_trunc( day, start_time )    as event_dt
    from
        &{l_raw_db}.&{l_raw_schema}.automatic_clustering_history_stg
    union
    select distinct
        date_trunc( day, start_time )    as event_dt
    from
        &{l_raw_db}.&{l_raw_schema}.materialized_view_refresh_history_stg
    union
    select distinct
        date_trunc( day, start_time )    as event_dt
    from
        &{l_raw_db}.&{l_raw_schema}.pipe_usage_history_stg
    union
    select distinct
        date_trunc( day, start_time )    as event_dt
    from
        &{l_raw_db}.&{l_raw_schema}.query_history_stg
    union
    select distinct
        date_trunc( day, start_time )    as event_dt
    from
        &{l_raw_db}.&{l_raw_schema}.replication_usage_history_stg
    union
    select distinct
        date_trunc( day, start_time )    as event_dt
    from
        &{l_raw_db}.&{l_raw_schema}.data_transfer_history_stg
)
select
     event_dt
    ,current_timestamp()            as dw_load_ts
from
    l_delta_date
order by
    1
;
