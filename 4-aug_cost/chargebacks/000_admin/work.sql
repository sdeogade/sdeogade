

put file:///Users/gsitzman/test/team_warehouse_lkp.csv @gsitzman_db.cost_trans.account_usage_stg;

put file:///Users/gsitzman/account/team_warehouse_lkp.csv @gsitzman_db.cost_trans.account_usage_stg;

list @gsitzman_db.cost_trans.account_usage_stg;

--team, Warehouse, BasicRole, AdminRole, GrantedRole, InheritedRole

drop schema gsitzman_db.cost_trans;

use role sysadmin;
grant imported privileges on database snowflake to role gsdmed_prd_sysadmin_fr;

use role gsdmed_prd_sysadmin_fr;

select current_warehouse()
;

insert into be_&{env}_pl_db.main.customer_dm
select
    ...
from
    be_&{env}_rl_erp_db.om.customer cst
    left join be_&{env}_il_db.main.customer_segment_s css on
        css.customer_id = cst.customer_id
    left join be_&{env}_il_db.main.customer_ltv_s cls on
        cls.customer_id = cst.customer_id
where
    ...
;


snowsql -c myaccount -r myrole -D env=dev -f customer_dm_ld.sql -o output_file=customer_dm_ld.out