/*

cd "/Volumes/GoogleDrive/Shared drives/Professional Services/Service Delivery/Individual Folders/Greg Sitzman/3) In-Use/Data Cloud Deployment Framework/Scripts/Chargebacks/Account"

snowsql -c aws_cas2 -r mwebster_dev_sysadmin_fr -D l_env=dev -f chargebacks_ddl_orch.sql -o output_file=chargebacks_ddl_orch.out

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

!print set context -----------------------------------------------------------

!source 000_admin/&{l_env}_context_ddl.sql

!print execute ddl -----------------------------------------------------------

!source 000_admin/account_ddl_orch.sql


!print SUCCESS!
!quit
