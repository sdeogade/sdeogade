--------------------------------------------------------------------
--  Purpose:
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
create or replace procedure &{l_common_db}.&{l_common_schema}.dw_delta_date_ld_sp( P_SRC_TABLE varchar, P_SRC_COLUMN varchar )
returns varchar
language javascript
execute as caller
as
$$
    // variables
    var status          = 'success';
    var tracePos        = 'step 0';
    var sqlResult;
    var sqlCmd;

    try {
        // insert distinct dates into work table
        tracePos  = 'insert into';
        sqlCmd    = `
            insert overwrite into
                --&{l_common_db}.&{l_common_schema}.dw_delta_date
                &{l_common_db}.&{l_common_schema}.dw_delta_date
            select distinct
                 to_date( ${P_SRC_COLUMN} )
                ,current_timestamp()
            from
                ${P_SRC_TABLE};
            `;

        sqlResult = snowflake.execute( { sqlText: sqlCmd } );

    }
    catch (err) {
        status  = 'line: ' + tracePos;
        status += '\n failed: code: ' + err.code + '\n state: ' + err.state;
        status += '\n message: ' + err.message;
        status += '\n stack trace:\n' + err.stacktracetxt;
    }
    finally {
        return status;
    }
$$
;
