--------------------------------------------------------------------
--  Purpose: load lookup table with an insert-only pattern since the 
--           pk has the potential of changing.
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
-- mapping match patterns and priorities

truncate table &{l_raw_db}.&{l_raw_schema}.be_budget_cost_lkp;

insert overwrite into 
    &{l_raw_db}.&{l_raw_schema}.be_budget_cost_lkp
with l_mapping as
(
    select to_char( null ) as be_name, to_date( null ) as month_begin_dt, to_number( null ) as compute_credit_cnt
    -- 
    union all select 'plan_lde', to_date('5/1/2021','mm/dd/yyyy'),  3426 
    union all select 'plan_lde', to_date('6/1/2021','mm/dd/yyyy'),  3426 
    union all select 'plan_lde', to_date('7/1/2021','mm/dd/yyyy'),  3953
    union all select 'plan_lde', to_date('8/1/2021','mm/dd/yyyy'),  4744 
    union all select 'plan_lde', to_date('9/1/2021','mm/dd/yyyy'),  5271
    union all select 'plan_lde', to_date('10/1/2021','mm/dd/yyyy'), 5534 
    union all select 'plan_lde', to_date('11/1/2021','mm/dd/yyyy'), 5534 
    union all select 'plan_lde', to_date('12/1/2021','mm/dd/yyyy'), 5798 
    union all select 'plan_lde', to_date('1/1/2022','mm/dd/yyyy'),  6061 
    union all select 'plan_lde', to_date('2/1/2022','mm/dd/yyyy'),  6325 
    union all select 'plan_lde', to_date('3/1/2022','mm/dd/yyyy'),  6589 
    union all select 'plan_lde', to_date('4/1/2022','mm/dd/yyyy'),  6589 
    --
    union all select 'exec_wms', to_date('5/1/2021','mm/dd/yyyy'),  1246 
    union all select 'exec_wms', to_date('6/1/2021','mm/dd/yyyy'),  1246 
    union all select 'exec_wms', to_date('7/1/2021','mm/dd/yyyy'),  1438 
    union all select 'exec_wms', to_date('8/1/2021','mm/dd/yyyy'),  1725 
    union all select 'exec_wms', to_date('9/1/2021','mm/dd/yyyy'),  1917 
    union all select 'exec_wms', to_date('10/1/2021','mm/dd/yyyy'), 2013 
    union all select 'exec_wms', to_date('11/1/2021','mm/dd/yyyy'), 2013 
    union all select 'exec_wms', to_date('12/1/2021','mm/dd/yyyy'), 2108 
    union all select 'exec_wms', to_date('1/1/2022','mm/dd/yyyy'),  2204 
    union all select 'exec_wms', to_date('2/1/2022','mm/dd/yyyy'),  2300 
    union all select 'exec_wms', to_date('3/1/2022','mm/dd/yyyy'),  2396 
    union all select 'exec_wms', to_date('4/1/2022','mm/dd/yyyy'),  2396 
    --
    union all select 'plat_lct', to_date('5/1/2021','mm/dd/yyyy'),  1557 
    union all select 'plat_lct', to_date('6/1/2021','mm/dd/yyyy'),  1557 
    union all select 'plat_lct', to_date('7/1/2021','mm/dd/yyyy'),  1797 
    union all select 'plat_lct', to_date('8/1/2021','mm/dd/yyyy'),  2156 
    union all select 'plat_lct', to_date('9/1/2021','mm/dd/yyyy'),  2396 
    union all select 'plat_lct', to_date('10/1/2021','mm/dd/yyyy'), 2516 
    union all select 'plat_lct', to_date('11/1/2021','mm/dd/yyyy'), 2516 
    union all select 'plat_lct', to_date('12/1/2021','mm/dd/yyyy'), 2635 
    union all select 'plat_lct', to_date('1/1/2022','mm/dd/yyyy'),  2755 
    union all select 'plat_lct', to_date('2/1/2022','mm/dd/yyyy'),  2875 
    union all select 'plat_lct', to_date('3/1/2022','mm/dd/yyyy'),  2995 
    union all select 'plat_lct', to_date('4/1/2022','mm/dd/yyyy'),  2995 
    --
    union all select 'plat_lvds', to_date('5/1/2021','mm/dd/yyyy'),  500 
    union all select 'plat_lvds', to_date('6/1/2021','mm/dd/yyyy'),  500 
    union all select 'plat_lvds', to_date('7/1/2021','mm/dd/yyyy'),  500 
    union all select 'plat_lvds', to_date('8/1/2021','mm/dd/yyyy'),  500 
    union all select 'plat_lvds', to_date('9/1/2021','mm/dd/yyyy'),  500 
    union all select 'plat_lvds', to_date('10/1/2021','mm/dd/yyyy'), 500 
    union all select 'plat_lvds', to_date('11/1/2021','mm/dd/yyyy'), 500 
    union all select 'plat_lvds', to_date('12/1/2021','mm/dd/yyyy'), 500 
    union all select 'plat_lvds', to_date('1/1/2022','mm/dd/yyyy'),  500 
    union all select 'plat_lvds', to_date('2/1/2022','mm/dd/yyyy'),  500 
    union all select 'plat_lvds', to_date('3/1/2022','mm/dd/yyyy'),  500 
    union all select 'plat_lvds', to_date('4/1/2022','mm/dd/yyyy'),  500 
    --
    union all select 'is_dms', to_date('5/1/2021','mm/dd/yyyy'),  1000 
    union all select 'is_dms', to_date('6/1/2021','mm/dd/yyyy'),  1000 
    union all select 'is_dms', to_date('7/1/2021','mm/dd/yyyy'),  1000 
    union all select 'is_dms', to_date('8/1/2021','mm/dd/yyyy'),  1000 
    union all select 'is_dms', to_date('9/1/2021','mm/dd/yyyy'),  1000 
    union all select 'is_dms', to_date('10/1/2021','mm/dd/yyyy'), 1000 
    union all select 'is_dms', to_date('11/1/2021','mm/dd/yyyy'), 1000 
    union all select 'is_dms', to_date('12/1/2021','mm/dd/yyyy'), 1000 
    union all select 'is_dms', to_date('1/1/2022','mm/dd/yyyy'),  1000 
    union all select 'is_dms', to_date('2/1/2022','mm/dd/yyyy'),  1000 
    union all select 'is_dms', to_date('3/1/2022','mm/dd/yyyy'),  1000 
    union all select 'is_dms', to_date('4/1/2022','mm/dd/yyyy'),  1000 
    --
    union all select 'plat_lui', to_date('5/1/2021','mm/dd/yyyy'),  0 
    union all select 'plat_lui', to_date('6/1/2021','mm/dd/yyyy'),  0 
    union all select 'plat_lui', to_date('7/1/2021','mm/dd/yyyy'),  500 
    union all select 'plat_lui', to_date('8/1/2021','mm/dd/yyyy'),  500 
    union all select 'plat_lui', to_date('9/1/2021','mm/dd/yyyy'),  500 
    union all select 'plat_lui', to_date('10/1/2021','mm/dd/yyyy'), 500 
    union all select 'plat_lui', to_date('11/1/2021','mm/dd/yyyy'), 500 
    union all select 'plat_lui', to_date('12/1/2021','mm/dd/yyyy'), 500 
    union all select 'plat_lui', to_date('1/1/2022','mm/dd/yyyy'),  500 
    union all select 'plat_lui', to_date('2/1/2022','mm/dd/yyyy'),  500 
    union all select 'plat_lui', to_date('3/1/2022','mm/dd/yyyy'),  500 
    union all select 'plat_lui', to_date('4/1/2022','mm/dd/yyyy'),  500 
    --
    
    union all select 'plan_retail', to_date('5/1/2021','mm/dd/yyyy'),  0 
    union all select 'plan_retail', to_date('6/1/2021','mm/dd/yyyy'),  0 
    union all select 'plan_retail', to_date('7/1/2021','mm/dd/yyyy'),  500 
    union all select 'plan_retail', to_date('8/1/2021','mm/dd/yyyy'),  500 
    union all select 'plan_retail', to_date('9/1/2021','mm/dd/yyyy'),  500 
    union all select 'plan_retail', to_date('10/1/2021','mm/dd/yyyy'), 500 
    union all select 'plan_retail', to_date('11/1/2021','mm/dd/yyyy'), 500 
    union all select 'plan_retail', to_date('12/1/2021','mm/dd/yyyy'), 500 
    union all select 'plan_retail', to_date('1/1/2022','mm/dd/yyyy'),  500 
    union all select 'plan_retail', to_date('2/1/2022','mm/dd/yyyy'),  500 
    union all select 'plan_retail', to_date('3/1/2022','mm/dd/yyyy'),  500 
    union all select 'plan_retail', to_date('4/1/2022','mm/dd/yyyy'),  500 
    --
)
select
     lm.be_name
    ,lm.month_begin_dt
    ,lm.compute_credit_cnt
    ,current_timestamp()    as dw_load_ts
from
    l_mapping lm
where
    lm.be_name is not null
;
