--This script aggregate by day the events from users, who installed our apps in last N days.  
--Step 1: Run and validate the query below in bigquery 
--Step 2: Create a daily schedule job(aka: schedule_job_A) to write the query result to table_A which contains results from all execution of the schedule job 
 --so their will be duplicate records in table A, the dedup script is in dedup.sql. Save the name of table_A for usage in dedup.sql 
with event_window as ( 
select current_date() as event_date union all 
select date_sub(current_date(), interval 1 day) union all 
select date_sub(current_date(), interval 2 day) union all 
select date_sub(current_date(), interval 3 day) union all 
select date_sub(current_date(), interval 4 day) union all 
select date_sub(current_date(), interval 5 day) union all 
select date_sub(current_date(), interval 6 day) union all 
select date_sub(current_date(), interval 7 day) union all 
select date_sub(current_date(), interval 8 day) union all 
select date_sub(current_date(), interval 9 day) union all 
select date_sub(current_date(), interval 10 day) union all 
select date_sub(current_date(), interval 11 day) union all 
select date_sub(current_date(), interval 12 day) union all 
select date_sub(current_date(), interval 13 day) union all 
select date_sub(current_date(), interval 14 day) union all 
select date_sub(current_date(), interval 15 day) union all 
select date_sub(current_date(), interval 16 day) union all 
select date_sub(current_date(), interval 17 day) union all 
select date_sub(current_date(), interval 18 day) union all 
select date_sub(current_date(), interval 19 day) union all 
select date_sub(current_date(), interval 20 day) union all 
select date_sub(current_date(), interval 21 day) union all 
select date_sub(current_date(), interval 22 day) union all 
select date_sub(current_date(), interval 23 day) union all 
select date_sub(current_date(), interval 24 day) union all 
select date_sub(current_date(), interval 25 day) union all 
select date_sub(current_date(), interval 26 day) union all 
select date_sub(current_date(), interval 27 day) union all 
select date_sub(current_date(), interval 28 day) union all 
select date_sub(current_date(), interval 29 day) union all 
select date_sub(current_date(), interval 30 day) union all 
select date_sub(current_date(), interval 31 day) union all 
select date_sub(current_date(), interval 32 day) union all 
select date_sub(current_date(), interval 33 day) union all 
select date_sub(current_date(), interval 34 day) union all 
select date_sub(current_date(), interval 35 day) union all 
select date_sub(current_date(), interval 36 day) union all 
select date_sub(current_date(), interval 37 day) union all 
select date_sub(current_date(), interval 38 day) union all 
select date_sub(current_date(), interval 39 day) union all 
select date_sub(current_date(), interval 40 day) union all 
select date_sub(current_date(), interval 41 day) union all 
select date_sub(current_date(), interval 42 day) union all 
select date_sub(current_date(), interval 43 day) union all 
select date_sub(current_date(), interval 44 day) union all 
select date_sub(current_date(), interval 45 day) union all 
select date_sub(current_date(), interval 46 day) union all 
select date_sub(current_date(), interval 47 day) union all 
select date_sub(current_date(), interval 48 day) union all 
select date_sub(current_date(), interval 49 day) union all 
select date_sub(current_date(), interval 50 day) union all 
select date_sub(current_date(), interval 51 day) union all 
select date_sub(current_date(), interval 52 day) union all 
select date_sub(current_date(), interval 53 day) union all 
select date_sub(current_date(), interval 54 day) union all 
select date_sub(current_date(), interval 55 day) union all 
select date_sub(current_date(), interval 56 day) union all 
select date_sub(current_date(), interval 57 day) union all 
select date_sub(current_date(), interval 58 day) union all 
select date_sub(current_date(), interval 59 day) union all 
select date_sub(current_date(), interval 60 day) 
), 
distinct_user_install_event as ( 
select 
distinct 
device.advertising_id as universal_user_id, 
parse_date("%Y%m%d",event_date)  as install_date 
from `west-game-mocha.sample.mocha_sample_events` 
where parse_date("%Y%m%d",event_date)  >= date_sub(current_date(), interval 60 day) 
and event_name= 'first_open' 
), 
distinct_user_install_event_with_next as ( 
select 
universal_user_id,install_date, 
ifnull(lag(install_date) over (partition by universal_user_id order by install_date desc),date('9999-12-31')) as next_install_date, 
from distinct_user_install_event
), 
full_user_date as ( 
select 
a.universal_user_id, 
a.install_date, 
b.event_date 
from distinct_user_install_event_with_next a 
cross join event_window b 
where b.event_date >= a.install_date 
and b.event_date < a.next_install_date 
order by universal_user_id, event_date 
), 
data_flat_1 as ( 
select 
device.advertising_id as universal_user_id, 
event_timestamp, 
event_name, 
parse_date("%Y%m%d",event_date)  as event_date, 
if (event_name in ('session_start'), 1 , 0) as login_flag, 
 if (event_name in ('in_app_purchase') , 1 , 0) as pay_flag, 
if (event_name='user_logged_in',1,0) as user_logged_in__flag,
if (event_name='session_start',1,0) as session_start__count,
if (event_name='app_remove',1,0) as app_remove__count,
if (event_name='screen_view',1,0) as screen_view__count,
if (event_name='spend_virtual_currency' and event_params.key='value',event_params.value.int_value,null) as spend_virtual_currency__value,
if (event_name='in_app_purchase',event_value_in_usd,null) as in_app_purchase__event_value_in_usd
from `west-game-mocha.sample.mocha_sample_events` ,unnest(event_params) as event_params 
where parse_date("%Y%m%d",event_date)  >= date_sub(current_date(), interval 60 day) 
and event_name in('user_logged_in','session_start','app_remove','screen_view','spend_virtual_currency','in_app_purchase')
), 
data_flat_2 as ( 
select 
universal_user_id, 
event_name, 
event_timestamp, 
event_date, 
max(login_flag) as login_flag, 
max(pay_flag) as pay_flag, 
max(user_logged_in__flag) as user_logged_in__flag, 
max(session_start__count) as session_start__count, 
max(app_remove__count) as app_remove__count, 
max(screen_view__count) as screen_view__count, 
max(spend_virtual_currency__value) as spend_virtual_currency__value, 
max(in_app_purchase__event_value_in_usd) as in_app_purchase__event_value_in_usd,
from data_flat_1
group by universal_user_id, event_name, event_date,event_timestamp 
), 
event_agg_by_day as ( 
select 
universal_user_id,
event_date, 
if(sum(login_flag)=0, 0,1) as login_flag, 
if(sum(pay_flag)=0, 0, 1 ) as pay_flag, 
if(sum(case when event_name = 'user_logged_in' then user_logged_in__flag end)>0,1,0) as user_logged_in__flag,
ifnull(sum(case when event_name = 'session_start' then session_start__count end),0) as session_start__count,
ifnull(sum(case when event_name = 'app_remove' then app_remove__count end),0) as app_remove__count,
ifnull(sum(case when event_name = 'screen_view' then screen_view__count end),0) as screen_view__count,
ifnull(sum(case when event_name = 'spend_virtual_currency' then spend_virtual_currency__value end),0) as spend_virtual_currency__value,
ifnull(sum(case when event_name = 'in_app_purchase' then in_app_purchase__event_value_in_usd end),0) as in_app_purchase__event_value_in_usd
from data_flat_2
group by universal_user_id ,event_date 
) 
select 
a.*, 
ifnull(b.login_flag,0) as login_flag, 
ifnull(b.pay_flag,0) as pay_flag, 
 ifnull(b.user_logged_in__flag,0) as user_logged_in__flag,
ifnull(b.session_start__count,0) as session_start__count,
ifnull(b.app_remove__count,0) as app_remove__count,
ifnull(b.screen_view__count,0) as screen_view__count,
ifnull(b.spend_virtual_currency__value,0) as spend_virtual_currency__value,
ifnull(b.in_app_purchase__event_value_in_usd,0) as in_app_purchase__event_value_in_usd,
current_timestamp() as collect_time 
from full_user_date a 
left join event_agg_by_day b 
on a.universal_user_id=b.universal_user_id
and a.event_date=b.event_date