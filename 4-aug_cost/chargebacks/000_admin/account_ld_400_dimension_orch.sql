------------------------------------------------------------------------------
-- 400_dimension
--
!print 400_dimension

-- load dimension tables using raw thru derivation
!print 400_dimension Step 1

!source 400_dimension/cb_account_dm_ld.sql
!source 400_dimension/cb_resource_dm_ld.sql
!source 400_dimension/cb_service_type_dm_ld.sql
!source 400_dimension/date_dm_ld.sql
!source 400_dimension/cb_be_dm_ld.sql

!print SUCCESS!
