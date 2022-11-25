--------------------------------------------------------------------
--  Purpose:
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
create or replace function &{l_common_db}.&{l_common_schema}.sf_compute_cost_f
(
     p_dw_account_shk   binary
    ,p_date             date
)
returns float
as
$$
    select
        max( cost_amt )
    from
        &{l_il_db}.&{l_il_schema}.sf_compute_cost_s
    where
            dw_account_shk    = p_dw_account_shk
        and p_date           >= active_dt
        and p_date            < inactive_dt
$$
;
