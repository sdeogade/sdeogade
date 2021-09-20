------------------------------------------------------------------------------
-- 300_integration
--
!print 300_integration

-- maintain distinct set of source accounts and source resources (warehouses and databases)
-- used by derivation, dimension and fact table load scripts
!print 300_integration Step 1

!source 300_integration/cb_account_h_ld.sql
!source 300_integration/cb_resource_h_ld.sql
!source 300_integration/cb_be_h_ld.sql

-- maintain relationships between source accounts and source resources.
-- used by derivation, dimension and fact table load scripts
!print 300_integration Step 2

!source 300_integration/cb_account_resource_l_ld.sql

!print SUCCESS!
