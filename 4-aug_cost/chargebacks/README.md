Cost Monitoring and Chargeback Scripts
======================================

DEV Environment
---------------
`000_admin/dev_context_d*.sql`

For Development purpose. Will only run inside the `ov40102.east-us-2.azure`
development snowflake account. The account will be used for acquisition and also
for the load of the data.  The scripts will use the following databases:

- `DEV_PLAT_GOV_COMMON_DB`
- `DEV_PLAT_GOV_RL_DB`
- `DEV_PLAT_GOV_IL_DB`
- `DEV_PLAT_GOV_PL_DB`

with the warehouse `DEV_PLAT_GOV_WH`.

The development scripts will use the following azure blob storage account
`azure://bysnow.blob.core.windows.net/admin/dev_account_usage/` as an external stage.


PRD Environment
---------------
`000_admin/prd_context_d*.sql`

The production chargeback environment. The scripts collect data from the `ov40102.east-us-2.azure`,
`by-euw_prd` and `by-admin_prd` accounts for aquisition and use the `by-admin_prd` account
for the load of the data. The scripts will use the following databases:

- `PRD_PLAT_GOV_COMMON_DB`
- `PRD_PLAT_GOV_RL_DB`
- `PRD_PLAT_GOV_IL_DB`
- `PRD_PLAT_GOV_PL_DB`

with the warehouse `PRD_PLAT_GOV_WH`.

The production scripts will use the following azure blob storage account
`azure://bysnow.blob.core.windows.net/admin/prd_account_usage/` as an external stage.
