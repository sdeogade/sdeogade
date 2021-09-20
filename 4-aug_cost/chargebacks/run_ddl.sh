#!/bin/bash


# DEV
snowsql  -c by_dev -r SYSADMIN -D l_env=dev -f chargebacks_ddl_orch.sql -o output_file=chargebacks_ddl_orch.out


# PRD
#snowsql  -c by_org -r SYSADMIN -D l_env=prd -f chargebacks_ddl_orch.sql -o output_file=chargebacks_ddl_orch.out

#snowsql  -c by_euw_prd -r SYSADMIN -D l_env=prd -f chargebacks_ddl_orch.sql -o output_file=chargebacks_ddl_orch.out
