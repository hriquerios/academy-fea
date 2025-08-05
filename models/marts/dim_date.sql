with
    dates_raw as (
        {{
            dbt_utils.date_spine(
                datepart="day"
                , start_date="cast('2010-01-01' as date)" 
                , end_date="cast('2015-12-31' as date)"
            )
        }}
    )
    
    , days_info as (
        select 
            cast(date_day as date) as date_day
            , extract(dayofweek from date_day) as day_of_week
            , extract(month from date_day) as month_number
            , extract(dayofyear from date_day) as day_of_year
            , extract(year from date_day) as year_number
            , extract(quarter from date_day) as quarter_number
            , extract(week from date_day) as week_number
        from dates_raw
    )
    
    , days_named as (
        select
            hash(date_day) as sk_date
            , date_day
            , day_of_week
            , month_number
            , day_of_year
            , year_number
            , quarter_number
            , week_number
            , case
                when day_of_week = 0 then 'Sunday'
                when day_of_week = 1 then 'Monday'
                when day_of_week = 2 then 'Tuesday'
                when day_of_week = 3 then 'Wednesday'
                when day_of_week = 4 then 'Thursday'
                when day_of_week = 5 then 'Friday'
                else 'Saturday' 
            end as day_name
            , case
                when month_number = 1 then 'January'
                when month_number = 2 then 'February'
                when month_number = 3 then 'March'
                when month_number = 4 then 'April'
                when month_number = 5 then 'May'
                when month_number = 6 then 'June'
                when month_number = 7 then 'July'
                when month_number = 8 then 'August'
                when month_number = 9 then 'September'
                when month_number = 10 then 'October'
                when month_number = 11 then 'November'
                else 'December' 
            end as month_name
            , case
                when month_number = 1 then 'Jan'
                when month_number = 2 then 'Feb'
                when month_number = 3 then 'Mar'
                when month_number = 4 then 'Apr'
                when month_number = 5 then 'May'
                when month_number = 6 then 'Jun'
                when month_number = 7 then 'Jul'
                when month_number = 8 then 'Aug'
                when month_number = 9 then 'Sep'
                when month_number = 10 then 'Oct'
                when month_number = 11 then 'Nov'
                else 'Dec' 
            end as month_abbreviation
            , concat('Q', quarter_number) as quarter_name
            , case 
                when day_of_week in (1, 7) then 'Weekend'
                else 'Weekday'
            end as weekday_weekend
        from days_info
    )

select *
from days_named