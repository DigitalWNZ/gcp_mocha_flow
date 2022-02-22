--This script calcualte the rolling_sum as of event_date for each event of each customer based on table_B which is the destination table in schedule_job_B. 
--Step 1: Change xxxxx in sql below to table_B. 
--Step 2: Run and validate the query in bigquery 
--Step 3: Create a schedule job(aka:schedule_job_C) to write the query  result to table_C.
select 
universal_user_id,
install_date,
event_date,
login_flag, 
pay_flag, 
platform, 
country, 
sum(click_ad__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as click_ad__count__sum,
login__flag,
sum(level_up__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as level_up__count__sum,
sum(logout__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as logout__count__sum,
purchase__total_price,
first_open__flag
from `xxxxx` 
