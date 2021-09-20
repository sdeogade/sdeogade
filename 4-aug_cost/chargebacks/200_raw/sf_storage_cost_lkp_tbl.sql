--------------------------------------------------------------------
--  Purpose: create psa tables
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------

--
-- permanent table with retention days
--
create table if not exists &{l_raw_db}.&{l_raw_schema}.sf_storage_cost_lkp
(
     organization_name              varchar( 250 )      not NULL
	 ,account_name                   varchar( 250 )      NOT null
    ,region_name                    varchar( 250 )      NOT null
    ,cost_amt                       float               not null
    ,active_dt                      date                not null
    ,inactive_dt                    date                not null
    --
    ,dw_load_ts                     timestamp_ltz       not null
)
data_retention_time_in_days = 1
;
