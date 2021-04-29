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
user_logged_in__flag,
sum(session_start__count) over (partiton by universal_user_id,event_date order by event_date asc rows unbounded preceding) as session_start__count__sum,
sum(app_remove__count) over (partiton by universal_user_id,event_date order by event_date asc rows unbounded preceding) as app_remove__count__sum,
sum(screen_view__count) over (partiton by universal_user_id,event_date order by event_date asc rows unbounded preceding) as screen_view__count__sum,
sum(spend_virtual_currency__value) over (partiton by universal_user_id,event_date order by event_date asc rows unbounded preceding) as spend_virtual_currency__value__sum,
in_app_purchase__event_value_in_usd
from `xxxxx` 
order by universal_user_id, event_date 
