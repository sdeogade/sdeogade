--------------------------------------------------------------------
--  Purpose: load lookup table with an insert-only pattern since the 
--           pk has the potential of changing.
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
--
-- update modified and insert new
--
merge into &{l_raw_db}.&{l_raw_schema}.cb_service_type_lkp t using
(
    with l_stg as
    (
        select
             sha1_binary( s.service_type_cd )   as dw_service_type_shk
            ,sha1_binary( concat( s.service_type_cd
                                 ,'|', s.service_type_name
                                 ,'|', s.service_type_group_name
                                 ,'|', to_char( s.active_dt, 'yyyymmdd' )
                                 ,'|', to_char( s.inactive_dt, 'yyyymmdd' )
                                )
                        )                       as dw_hash_diff
            ,current_timestamp()                as dw_update_ts
            --
            ,s.*
        from
            ( values
                   ( 'AUTOMATIC_CLUSTERING',      'Automatic Clustering',      'Service Metering',   '2021-05-01'::date, '2022-04-30'::date)
                  ,( 'MATERIALIZED_VIEW',         'Materialized View',         'Service Metering',   '2021-05-01'::date, '2022-04-30'::date)
                  ,( 'PIPE',                      'Snowpipe',                  'Service Metering',   '2021-05-01'::date, '2022-04-30'::date)
                  ,( 'REPLICATION_METERING',      'Replication Metering',      'Service Metering',   '2021-05-01'::date, '2022-04-30'::date)
                  ,( 'SEARCH_OPTIMIZATION',       'Search Optimization',       'Service Metering',   '2021-05-01'::date, '2022-04-30'::date)
                  ,( 'WAREHOUSE_METERING',        'Warehouse Metering',        'Warehouse Metering', '2021-05-01'::date, '2022-04-30'::date)
                  ,( 'WAREHOUSE_METERING_READER', 'Warehouse Metering Reader', 'Warehouse Metering', '2021-05-01'::date, '2022-04-30'::date)
                  --                                                                                                              
                  ,( 'STORAGE_DATABASE',          'Storage Database',          'Storage',            '2021-05-01'::date, '2022-04-30'::date)
                  ,( 'STORAGE_STAGE',             'Storage Internal Stage',    'Storage',            '2021-05-01'::date, '2022-04-30'::date)
                  --                                                                                                             
                  ,( 'DATA_TRANSFER',             'Data Transfer',             'Data Transfer',      '2021-05-01'::date, '2022-04-30'::date)
                  ,( 'REPLICATION_DATA_TRANSFER', 'Replication Data Transfer', 'Data Transfer',      '2021-05-01'::date, '2022-04-30'::date)
            ) s ( service_type_cd, service_type_name, service_type_group_name, active_dt, inactive_dt )
    )
    ,l_deduped as
    (
        select
            *
        from
            (
            select
                 -- identify dupes and only keep copy 1
                 -- note this is deduping on the primary key
                 row_number() over( partition by s.dw_service_type_shk order by s.active_dt desc ) as seq_no
                ,s.*
            from
                l_stg s
            )
        where
            seq_no = 1 -- keep only unique rows
    )
    select
         current_timestamp()        as dw_version_ts
        ,s.*
    from
        l_deduped s
        left join &{l_raw_db}.&{l_raw_schema}.cb_service_type_lkp t on
            t.dw_service_type_shk = s.dw_service_type_shk
    where
        -- source row does not exist in target table
        t.dw_service_type_shk is null
        -- or source row is more recent and differs from target table
        or (
                t.dw_update_ts      < s.dw_update_ts
            and t.dw_hash_diff     != s.dw_hash_diff
           )
    order by
        s.active_dt
) s
on
(
    t.dw_service_type_shk = s.dw_service_type_shk
)
when matched then update set
     t.dw_hash_diff             = s.dw_hash_diff
    ,t.service_type_cd          = s.service_type_cd        
    ,t.service_type_name        = s.service_type_name      
    ,t.service_type_group_name  = s.service_type_group_name
    ,t.active_dt                = s.active_dt              
    ,t.inactive_dt              = s.inactive_dt
    --
    ,t.dw_update_ts             = current_timestamp()
when not matched then insert
(
     dw_service_type_shk
    ,dw_hash_diff
    ,service_type_cd        
    ,service_type_name      
    ,service_type_group_name
    ,active_dt              
    ,inactive_dt            
    ,dw_load_ts
    ,dw_update_ts
)
values
(
     s.dw_service_type_shk
    ,s.dw_hash_diff
    ,s.service_type_cd        
    ,s.service_type_name      
    ,s.service_type_group_name
    ,s.active_dt              
    ,s.inactive_dt            
    ,current_timestamp()
    ,current_timestamp()
)
;


