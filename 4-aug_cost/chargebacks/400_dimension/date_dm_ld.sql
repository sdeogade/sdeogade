--------------------------------------------------------------------
--  Purpose: data acquisition
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------

--------------------------------------------------------------------
-- date range to maintain
--
set (l_start_dt, l_end_dt, l_day_cnt, l_null_dt, l_undefined_dt ) = 
(
    select
         to_date( '01/01/2010', 'mm/dd/yyyy' )                     as start_dt
        ,date_trunc( year, dateadd( year,  4, current_date() ) )   as end_dt
        ,datediff( day, start_dt, end_dt )                         as day_cnt
        -- defaults for nulls and undefined values
        ,to_date( '01/01/1950', 'mm/dd/yyyy' )                     as null_dt
        ,to_date( '01/01/1900', 'mm/dd/yyyy' )                     as undefined_dt
);

insert overwrite into &{l_pl_db}.&{l_pl_schema}.date_dm
with l_date as
(
    -- generate broad date range to translate
    select
         cal_dt
        ,date_part( year, cal_dt )              as cal_year_no
        ,date_part( quarter, cal_dt )           as cal_year_quarter_no
        ,date_part( month, cal_dt )             as cal_year_month_no
        ,date_part( dayofyear, cal_dt )         as cal_year_day_no
        -- dates
        ,min( cal_dt ) over( partition by date_part( year, cal_dt ) )                                 as cal_year_dt
        ,min( cal_dt ) over( partition by date_part( year, cal_dt ), date_part( quarter, cal_dt ) )   as cal_quarter_dt
        ,min( cal_dt ) over( partition by date_part( year, cal_dt ), date_part( month, cal_dt ) )     as cal_month_dt
    from
        (
        select
            dateadd( day, seq4(), $l_start_dt )      as cal_dt
        from
            table( generator( rowcount => $l_day_cnt ) )
        -- union in null and undefined placeholders
        union all select $l_null_dt
        union all select $l_undefined_dt
        )
)
,l_cal_dt as
(
    --
    -- generate dates - full ISO years are generated.
    --
    select
         cal_dt
        ,&{l_common_db}.&{l_common_schema}.date_sid_f( cal_dt )                           as dw_date_sid
        ,&{l_common_db}.&{l_common_schema}.date_sid_f( dateadd( 'day',  -364, cal_dt ) )  as dw_cal_ly_date_sid
        ,to_char( cal_dt, 'yyyy/mm/dd Dy' )             as cal_date_label
        ,case
            when date_part( 'dow', cal_dt ) in ( 0,6 ) then 'Y'
            else 'N'
         end                                            as cal_weekend_fl
        ,case
            when date_part( 'dow', cal_dt ) in ( 0,6 ) then 'N'
            else 'Y'
         end                                            as cal_weekday_fl
        ,cal_year_no
        ,cal_year_dt
        ,cal_year_quarter_no
        ,min( case when cal_year_no = date_part( year, current_date ) then cal_quarter_dt else to_date( null ) end ) over( partition by cal_year_quarter_no )    as cal_year_quarter_dt
        ,cal_year_month_no
        ,min( case when cal_year_no = date_part( year, current_date ) then cal_month_dt else to_date( null ) end ) over( partition by cal_year_month_no )       as cal_year_month_dt
        ,to_char( cal_year_no )
          || '/'
          || lpad( date_part( 'mm', cal_dt ), 2, '0' )
          || '/01 '
          || monthname( cal_dt )                        as cal_month_label
        ,cal_year_day_no
        ,cal_quarter_dt
        ,to_char( cal_quarter_dt, 'yyyy/mm/dd' )
                     || ' Q'
                     || cal_year_quarter_no             as cal_quarter_label
        ,dense_rank()
                over ( partition by
                        cal_year_no
                       ,cal_year_quarter_no
                    order by
                        cal_year_month_no    )          as cal_quarter_month_no
        ,dense_rank()
                over ( partition by
                           cal_year_no
                          ,cal_year_quarter_no
                       order by
                           cal_year_day_no      )       as cal_quarter_day_no
        ,cal_month_dt
        ,dense_rank()
             over ( partition by
                        cal_year_no
                       ,cal_year_month_no
                    order by
                        cal_year_day_no   )             as cal_month_day_no
        ,date_part( 'dow', cal_dt ) + 1                 as cal_week_day_no
        ,dense_rank()
              over ( order by
                          cal_year_no
                         ,cal_year_quarter_no )         as cal_itd_quarter_no
        ,dense_rank()
              over ( order by
                          cal_year_no
                         ,cal_year_month_no )           as cal_itd_month_no
        ,dense_rank()
              over ( order by
                          cal_dt )                      as cal_itd_day_no
    from
        l_date
)
,l_iso_dt as
(
    --
    -- generate iso periods - this includes iso_year_<period>_dt for quarter, month and week
    --
    select
         cal_dt
        ,iso_year_no
        ,iso_year_quarter_no
        --
        -- year dates based on current iso year for todays date
        --
        ,min( case when iso_year_no = iso_current_year_no then iso_quarter_dt else to_date( null ) end ) over( partition by iso_year_quarter_no )    as iso_year_quarter_dt
        ,min( case when iso_year_no = iso_current_year_no then iso_month_dt   else to_date( null ) end ) over( partition by iso_year_month_no )      as iso_year_month_dt
        ,min( case when iso_year_no = iso_current_year_no then iso_week_dt    else to_date( null ) end ) over( partition by iso_year_week_no )       as iso_year_week_dt
        ,to_char( iso_quarter_dt, 'yyyy/mm/dd' )
                     || ' Q'
                     || iso_year_quarter_no                      as iso_quarter_label
        ,iso_year_month_no
        ,to_char( iso_month_dt, 'yyyy/mm/dd' )
                     || ' '
                     || monthname( iso_month_dt )                as iso_month_label
        ,iso_year_week_no
        ,iso_year_day_no
        -- dates
        ,iso_year_dt
        ,iso_quarter_dt
        ,dense_rank()
                over ( partition by
                           iso_year_no
                          ,iso_year_quarter_no
                       order by
                           iso_year_month_no    )                as iso_quarter_month_no
        ,dense_rank()
                over ( partition by
                           iso_year_no
                          ,iso_year_quarter_no
                       order by
                           iso_week_dt    )                      as iso_quarter_week_no
        ,dense_rank()
                over ( partition by
                           iso_year_no
                          ,iso_year_quarter_no
                       order by
                           iso_year_day_no      )                as iso_quarter_day_no
        ,iso_month_dt
        ,dense_rank()
                over ( partition by
                           iso_year_no
                          ,iso_year_month_no
                       order by
                           iso_week_dt    )                      as iso_month_week_no
        ,dense_rank()
                over ( partition by
                           iso_year_no
                          ,iso_year_month_no
                       order by
                           iso_year_day_no   )                   as iso_month_day_no
        ,iso_week_dt
        ,to_char( max( iso_week_dt )
                 over( partition by iso_year_no, iso_year_week_no )
                 ,'yyyy/mm/dd' )
            || ' Wk '
            || lpad( to_char( iso_year_week_no ), 2, '0' )       as iso_week_label
        ,date_part( 'dow_iso', cal_dt )                          as iso_week_day_no
        -- current iso year
        ,iso_current_year_no
        ,dense_rank()
              over ( order by
                          iso_year_no
                         ,iso_year_quarter_no )                    as iso_itd_quarter_no
         ,dense_rank()
              over ( order by
                          iso_year_no
                         ,iso_year_month_no )                      as iso_itd_month_no
         ,dense_rank()
              over ( order by
                          iso_year_no
                         ,iso_year_week_no )                       as iso_itd_week_no
         ,dense_rank()
              over ( order by
                          cal_dt )                                 as iso_itd_day_no
    from
        (
        select
             cal_dt
            ,iso_year_no
            ,iso_year_quarter_no
            ,iso_year_month_no
            ,iso_year_week_no
            ,iso_year_day_no
            -- dates
            ,date_from_parts( iso_year_no, 1, 1 )                   as iso_year_dt
            ,case iso_year_quarter_no
                when 1 then date_from_parts( iso_year_no,  1, 1 )
                when 2 then date_from_parts( iso_year_no,  4, 1 )
                when 3 then date_from_parts( iso_year_no,  7, 1 )
                when 4 then date_from_parts( iso_year_no, 10, 1 )
                else to_date( null )
             end                                                    as iso_quarter_dt
            ,date_from_parts( iso_year_no, iso_year_month_no, 1 )   as iso_month_dt
            ,iso_week_dt
            -- current iso year
            ,date_part( year, dateadd( day, 3, date_trunc( week, current_date ) ) )     as iso_current_year_no
        from
            (
            select
                 cal_dt
                ,date_part( year, dateadd( day, 3, iso_week_dt ) )       as iso_year_no
                --
                -- each quarter is 13 weeks with q4 sometimes being 14 weeks for 53 week years
                --
                ,case
                    when iso_year_week_no <= 13 then  1
                    when iso_year_week_no <= 26 then  2
                    when iso_year_week_no <= 39 then  3
                    when iso_year_week_no <= 53 then  4
                    else 5 -- should never hit
                 end                        as iso_year_quarter_no
                --
                -- months are driven by 4-5-4 quarters
                --
                ,case
                    when iso_year_week_no <=  4 then  1  -- jan 4
                    when iso_year_week_no <=  9 then  2  -- feb 5
                    when iso_year_week_no <= 13 then  3  -- mar 4
                    when iso_year_week_no <= 17 then  4  -- apr 4
                    when iso_year_week_no <= 22 then  5  -- may 5
                    when iso_year_week_no <= 26 then  6  -- jun 4
                    when iso_year_week_no <= 30 then  7  -- jul 4
                    when iso_year_week_no <= 35 then  8  -- aug 5
                    when iso_year_week_no <= 39 then  9  -- sep 4
                    when iso_year_week_no <= 43 then 10  -- oct 4
                    when iso_year_week_no <= 48 then 11  -- nov 5
                    when iso_year_week_no <= 53 then 12  -- dec 4
                    else 13 -- should never hit
                 end                        as iso_year_month_no
                ,iso_year_week_no
                ,iso_week_dt
                ,row_number() over( partition by date_part( year, dateadd( day, 3, iso_week_dt ) ) order by cal_dt )    as iso_year_day_no
            from
                (
                select
                     cal_dt
                    ,date_trunc( week, cal_dt )                                                 as iso_week_dt
                    ,date_part( week, cal_dt )                                                  as iso_year_week_no
                from
                    l_cal_dt
                )
            )
        )
)
select
    -- cal
     lcd.dw_date_sid
    ,lcd.dw_cal_ly_date_sid
    ,lcd.cal_dt
    ,lcd.cal_date_label
    ,lcd.cal_weekend_fl
    ,lcd.cal_weekday_fl
    -- iso
    ,lid.iso_year_no
    ,lid.iso_year_dt
    ,lid.iso_year_quarter_no
    ,lid.iso_year_quarter_dt
    ,lid.iso_year_month_no
    ,lid.iso_year_month_dt
    ,lid.iso_year_week_no
    ,lid.iso_year_week_dt
    ,lid.iso_year_day_no
    --
    ,lcd.cal_year_no
    ,lcd.cal_year_dt
    ,lcd.cal_year_quarter_no
    ,lcd.cal_year_quarter_dt
    ,lcd.cal_year_month_no
    ,lcd.cal_year_month_dt
    ,lcd.cal_year_day_no
    --
    ,lid.iso_quarter_dt
    ,lid.iso_quarter_label
    ,lid.iso_quarter_month_no
    ,lid.iso_quarter_week_no
    ,lid.iso_quarter_day_no
    --
    ,lcd.cal_quarter_dt
    ,lcd.cal_quarter_label
    ,lcd.cal_quarter_month_no
    ,dense_rank()
           over ( partition by
                      lcd.cal_year_no
                     ,lcd.cal_year_quarter_no
                  order by
                      lid.iso_week_dt    )        as cal_quarter_week_no
    ,lcd.cal_quarter_day_no
    --
    ,lid.iso_month_dt
    ,lid.iso_month_label
    ,lid.iso_month_week_no
    ,lid.iso_month_day_no
    --
    ,lcd.cal_month_dt
    ,lcd.cal_month_label
    ,dense_rank()
           over ( partition by
                      lcd.cal_year_no
                     ,lcd.cal_year_month_no
                  order by
                      lid.iso_week_dt    )       as cal_month_week_no
    ,lcd.cal_month_day_no
    --
    ,lid.iso_week_dt
    ,lid.iso_week_label
    ,lid.iso_week_day_no
    ,lcd.cal_week_day_no
    -- iso itd
    ,lid.iso_itd_quarter_no
    ,lid.iso_itd_month_no
    ,lid.iso_itd_week_no
    ,lid.iso_itd_day_no
    -- cal itd
    ,lcd.cal_itd_quarter_no
    ,lcd.cal_itd_month_no
    ,lcd.cal_itd_day_no
     --
    ,0                                                            as iso_ptd_bt
    ,'N'                                                          as iso_ptd_label
    ,0                                                            as cal_ptd_bt
    ,'N'                                                          as cal_ptd_label
    -- future
    ,0                                                            as cal_future_date_bt
--    ,nvl( dhs.holiday_label, '~' )                               as holiday_name
    ,'~'                                                          as holiday_name
    ,to_timestamp_ltz( current_timestamp() )                      as dw_load_ts
    ,to_timestamp_ltz( current_timestamp() )                      as dw_update_ts
from
    l_cal_dt        lcd
    join l_iso_dt   lid on
        lid.cal_dt = lcd.cal_dt
    --
    -- create a holiday satellite within the derivation layer and then join into this load script
    -- to populate holiday names.
    --
    --    left join date_holiday_s dhs on
    --        dhs.holiday_dt = lcd.cal_dt
where
    lcd.cal_dt is not null
order by
    lcd.cal_dt
;
