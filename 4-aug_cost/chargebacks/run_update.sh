#!/bin/bash

# DEV
snowsql -c by_dev -r SYSADMIN -D l_env=dev -f dev_chargebacks_ld_orch.sql -o output_file=chargebacks_ld_orch.out

# PRD
#snowsql -c by_org -r SYSADMIN -D l_env=prd -f chargebacks_ld_orch.sql -o output_file=chargebacks_ld_orch.out
