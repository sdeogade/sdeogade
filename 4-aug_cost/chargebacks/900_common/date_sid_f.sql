--------------------------------------------------------------------
--  Purpose: create psa tables
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
create or replace function &{l_common_db}.&{l_common_schema}.date_sid_f
(
    p_date             date
)
returns number
as
$$
    select to_number( to_char( p_date, 'yyyymmdd') )
$$
;
