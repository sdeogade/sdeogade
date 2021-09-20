
------------------ automatic_clustering_history --------------------------------------

set l_last_control_dt_ach =
(
    select
        ifnull( dateadd( hour, -4, max( start_time ) ), dateadd( month, -13, current_timestamp() ) ) as last_control_dt
    from
        &{l_raw_db}.&{l_raw_schema}.automatic_clustering_history
    where
            organization_name = upper( '&{l_org_name}' )
        and account_name      = upper( '&{l_account_name}' )
);


--------------------------

set l_last_control_dt_dth =
(
    select
        ifnull( dateadd( hour, -4, max( start_time ) ), dateadd( month, -13, current_timestamp() ) ) as last_control_dt
    from
        &{l_raw_db}.&{l_raw_schema}.data_transfer_history_acq
    where
            organization_name = upper( '&{l_org_name}' )
        and account_name      = upper( '&{l_account_name}' )
);


----------------------------


set l_last_control_dt_dsuh =
(
    select
        ifnull( dateadd( hour, -4, max( start_time ) ), dateadd( month, -13, current_timestamp() ) ) as last_control_dt
    from
        &{l_raw_db}.&{l_raw_schema}.database_storage_usage_history_acq
    where
            organization_name = upper( '&{l_org_name}' )
        and account_name      = upper( '&{l_account_name}' )
);


--------------------

set l_last_control_dt_mvrh =
(
    select
        ifnull( dateadd( hour, -4, max( start_time ) ), dateadd( month, -13, current_timestamp() ) ) as last_control_dt
    from
        &{l_raw_db}.&{l_raw_schema}.materialized_view_refresh_history_acq
    where
            organization_name = upper( '&{l_org_name}' )
        and account_name      = upper( '&{l_account_name}' )
);

-----------------------


set l_last_control_dt_mdh =
(
    select
        ifnull( dateadd( hour, -4, max( start_time ) ), dateadd( month, -13, current_timestamp() ) ) as last_control_dt
    from
        &{l_raw_db}.&{l_raw_schema}.metering_daily_history_acq
    where
            organization_name = upper( '&{l_org_name}' )
        and account_name      = upper( '&{l_account_name}' )
);


--------------------------


set l_last_control_dt_puh =
(
    select
        ifnull( dateadd( hour, -4, max( start_time ) ), dateadd( month, -13, current_timestamp() ) ) as last_control_dt
    from
        &{l_raw_db}.&{l_raw_schema}.pipe_usage_history_acq
    where
            organization_name = upper( '&{l_org_name}' )
        and account_name      = upper( '&{l_account_name}' )
);


-------------------------


set l_last_control_dt_qh =
(
    select
        ifnull( dateadd( hour, -4, max( start_time ) ), dateadd( month, -13, current_timestamp() ) ) as last_control_dt
    from
        &{l_raw_db}.&{l_raw_schema}.query_history_acq
    where
            organization_name = upper( '&{l_org_name}' )
        and account_name      = upper( '&{l_account_name}' )
);





-----------------------------

set l_last_control_dt_ruh =
(
    select
        ifnull( dateadd( hour, -4, max( start_time ) ), dateadd( month, -13, current_timestamp() ) ) as last_control_dt
    from
        &{l_raw_db}.&{l_raw_schema}.replication_usage_history_acq
    where
            organization_name = upper( '&{l_org_name}' )
        and account_name      = upper( '&{l_account_name}' )
);

------------------------------


set l_last_control_dt_soh =
(
    select
        ifnull( dateadd( hour, -4, max( start_time ) ), dateadd( month, -13, current_timestamp() ) ) as last_control_dt
    from
        &{l_raw_db}.&{l_raw_schema}.search_optimization_history_acq
    where
            organization_name = upper( '&{l_org_name}' )
        and account_name      = upper( '&{l_account_name}' )
);


-------------------------------


set l_last_control_dt_ssu =
(
    select
        ifnull( dateadd( hour, -4, max( start_time ) ), dateadd( month, -13, current_timestamp() ) ) as last_control_dt
    from
        &{l_raw_db}.&{l_raw_schema}.search_optimization_history_acq
    where
            organization_name = upper( '&{l_org_name}' )
        and account_name      = upper( '&{l_account_name}' )
);


-------------------------------


set l_last_control_dt_wlh =
(
    select
        ifnull( dateadd( hour, -4, max( start_time ) ), dateadd( month, -13, current_timestamp() ) ) as last_control_dt
    from
        &{l_raw_db}.&{l_raw_schema}.warehouse_load_history_acq
    where
            organization_name = upper( '&{l_org_name}' )
        and account_name      = upper( '&{l_account_name}' )
);


-------------------------------


set l_last_control_dt_wmh =
(
    select
        ifnull( dateadd( hour, -4, max( start_time ) ), dateadd( month, -13, current_timestamp() ) ) as last_control_dt
    from
        &{l_raw_db}.&{l_raw_schema}.warehouse_metering_history_acq
    where
            organization_name = upper( '&{l_org_name}' )
        and account_name      = upper( '&{l_account_name}' )
);


-------------------------------


set l_last_control_dt_wmhr =
(
    select
        ifnull( dateadd( hour, -4, max( start_time ) ), dateadd( month, -13, current_timestamp() ) ) as last_control_dt
    from
        &{l_raw_db}.&{l_raw_schema}.warehouse_metering_history_reader_acq
    where
            organization_name = upper( '&{l_org_name}' )
        and account_name      = upper( '&{l_account_name}' )
);


-------------------------------


set l_last_control_dt_qhr =
(
    select
        ifnull( dateadd( hour, -4, max( start_time ) ), dateadd( month, -13, current_timestamp() ) ) as last_control_dt
    from
        &{l_raw_db}.&{l_raw_schema}.query_history_reader_acq
    where
            organization_name = upper( '&{l_org_name}' )
        and account_name      = upper( '&{l_account_name}' )

---------------------------------