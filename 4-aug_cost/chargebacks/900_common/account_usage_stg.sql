--------------------------------------------------------------------
--  Purpose: Internal stage for accout_usage data
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------

---set variable_substitution=true. But set it to FALSE ---!set variable_substitution=true


create stage if not exists &{l_common_db}.&{l_common_schema}.account_usage_stg;


/*
url='azure://bysnow.blob.core.windows.net/admin/account_usage/'
credentials=(azure_sas_token="sp=racwdl&st=2021-06-01T00:00:00Z&se=2022-06-01T00:00:00Z&spr=https&sv=2020-02-10&sr=c&sig=xPN%2Bd7JGfYfLhAwiHeG%2B8ZitMOoDuBKDEvIyPlTlQoI%3D")
;

-- turn off session variable
*/


/*
--SET DATE_FOLDER = (SELECT REPLACE(TO_VARCHAR(TO_DATE(CURRENT_TIMESTAMP())),'-',''));
create or replace stage my_azure_stage
url='azure://bysnow.blob.core.windows.net/admin/account_usage/'
credentials=(azure_sas_token='sp=r&st=2021-06-01T00:00:00Z&se=2022-01-01T00:00:00Z&spr=https&sv=2020-02-10&sr=c&sig=U651MiPhO1PeCiOUqy%2FyTcAIHMB0VtciCYWiada2Cos%3D')
encryption=(type='AZURE_CSE' master_key = 'kPxX0jzYfIamtnJEUTHwq80Au6NbSgPH5r4BDDwOaO8=')
  file_format = my_csv_format;
*/