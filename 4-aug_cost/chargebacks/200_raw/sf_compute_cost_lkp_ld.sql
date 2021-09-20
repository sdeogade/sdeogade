--------------------------------------------------------------------
--  Purpose: load lookup table with an insert-only pattern since the 
--           pk has the potential of changing.
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
-- cost rows for each account and time period
insert overwrite into 
    &{l_raw_db}.&{l_raw_schema}.sf_compute_cost_lkp
select
     cst.organization_name
	,cst.account_name
    ,cst.region_name
    ,cst.cost_amt
    ,cst.active_dt
    ,cst.inactive_dt
    --
    ,current_timestamp()        as dw_load_ts
from
    ( values
	
		 ( 'BY','OV40102',		'AZURE_EASTUS2', 	2.37, 	'2021-05-01', '2022-04-30')
		 ,( 'BY','KL23518', 	'AZURE_EASTUS2', 	3.16, 	'2021-05-01', '2022-04-30')
         ,( 'BY','GL26249',		'AZURE_WESTEUROPE', 3.95, 	'2021-05-01', '2022-04-30')
         ,( 'BY','OV40102', 		'AZURE_EASTUS2', 	0.00,	'2021-01-01', '2021-04-30')
		 ,( 'BY','KL23518', 	'AZURE_EASTUS2', 	0.00, 	'2021-01-01', '2021-04-30')
         ,( 'BY', 'GL26249',	'AZURE_WESTEUROPE', 0.00, 	'2021-01-01', '2021-04-30')
         --KL23518  ADMIN_PRD
		 --OV40102  US_DEV
         --GL26249  EUW_PRD
        
    ) cst( organization_name, account_name, region_name, cost_amt, active_dt, inactive_dt)
order by
    cst.active_dt
;

