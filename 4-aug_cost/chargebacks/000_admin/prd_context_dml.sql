------------------------------------------------------------------------------
-- context
--
!print set environment context

!define l_common_db=PRD_PLAT_GOV_COMMON_DB
!define l_common_schema=UTIL

!define l_raw_db=PRD_PLAT_GOV_RL_DB
!define l_raw_schema=account_usage

!define l_il_db=PRD_PLAT_GOV_IL_DB
!define l_il_schema=USAGE_METRIC

!define l_pl_db=PRD_PLAT_GOV_PL_DB
!define l_pl_schema=REPORTING

use warehouse PRD_PLAT_GOV_WH;
alter warehouse PRD_PLAT_GOV_WH set warehouse_size=small;

!print SUCCESS!
