/*

cd "/Volumes/GoogleDrive/Shared drives/Professional Services/Service Delivery/Individual Folders/Greg Sitzman/3) In-Use/Data Cloud Deployment Framework/Scripts/Chargebacks/Account"

--C:\Users\<username>\.snowsql\cmd

--1--  snowsql  -c by_org -r SYSADMIN -D l_env=dev -f chargebacks_ddl_orch.sql -o output_file=chargebacks_ddl_orch.out



--2--  snowsql -c by_org -r SYSADMIN -D l_env=dev -f chargebacks_ld_orch.sql -o output_file=chargebacks_ld_orch.out

*/


------------------------------------------------------------------------------
-- snowsql session
--
!set variable_substitution=true
!set exit_on_error=true
!set echo=true

------------------------------------------------------------------------------
-- 000_admin
--
!print 000_admin

!print set context

!source 000_admin/&{l_env}_context_dml.sql

------------------------------------------------------------------------------
-- 000_admin acquisition
--
-- Add each source account to this section.  Make sure there is a connection
-- defined for each account within the snowsql config file.
--
!print 00_admin acquisition

-- set delta hours for fixed delta window scenario
-- 366 days = 8784 hours
!define l_delta_hours=8784

--------
--account_acq_100_set_last_control_date_from_org_account

!print 000_admin acquisition account 1 ---------------------------------------

!define l_account_name="apisero_dev"
!connect &{l_account_name}

-- org the snowflake account resides in
!define l_org_name='APISERO'
!source 000_admin/account_acq_100_acquisition_orch.sql

!disconnect

-- !print 000_admin acquisition account 2 ---------------------------------------

-- !define l_account_name="by_euw_prd"
-- !connect &{l_account_name}

-- -- org the snowflake account resides in
-- !define l_org_name='BY'
-- !source 000_admin/account_acq_100_acquisition_orch.sql

-- !disconnect

-- !print 000_admin acquisition account 3 ---------------------------------------

-- !define l_account_name="by_org"
-- !connect &{l_account_name}

-- -- org the snowflake account resides in
-- !define l_org_name='BY'
-- !source 000_admin/account_acq_100_acquisition_orch.sql

-- !disconnect


------------------------------------------------------------------------------
-- 000_admin load
--
-- Load all acquisition step files
--
!print 000_admin account load ------------------------------org account--------------------------

!define l_account_name="apisero_dev"
!connect &{l_account_name}

!source 000_admin/account_ld_000_admin_orch.sql
!source 000_admin/account_ld_200_raw_orch.sql
!source 000_admin/account_ld_300_integration_orch.sql
!source 000_admin/account_ld_310_derivation_orch.sql
!source 000_admin/account_ld_320_mdm_orch.sql
!source 000_admin/account_ld_400_dimension_orch.sql
!source 000_admin/account_ld_410_fact_atomic_orch.sql
!source 000_admin/account_ld_420_fact_aggregate_orch.sql
!source 000_admin/account_ld_500_outbound_orch.sql

!disconnect


!print SUCCESS!
!quit
