--This script aggregate by day the events from users, who installed our apps in last N days.  
--Step 1: Run and validate the query below in bigquery 
--Step 2: Create a daily schedule job(aka: schedule_job_A) to write the query result to table_A which contains results from all execution of the schedule job 
 --so their will be duplicate records in table A, the dedup script is in dedup.sql. Save the name of table_A for usage in dedup.sql 
CREATE TEMP FUNCTION
abstract_params(input_arr any type, key_value string,value_field int64) as ( 
(select
  case
     when value_field=1 then safe_cast(input_arr.value.string_value as float64)
     when value_field=2 then safe_cast(input_arr.value.int_value as float64)
     when value_field=3 then input_arr.value.float_value 
     else safe_cast(input_arr.value.double_value as float64)
  end
from unnest(input_arr) as input_arr
where key=key_value
limit 1)
);
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
user_id as universal_user_id, 
parse_date("%Y%m%d",event_date)  as install_date, 
platform, 
geo.country, 
count(0) 
from `allen-first.gaming_analytics.events` 
where parse_date("%Y%m%d",event_date)  >= date_sub(current_date(), interval 60 day) 
and event_name= 'first_open' 
group by 1,2,3,4), 
distinct_user_install_event_with_next as ( 
select 
universal_user_id,install_date, platform, country,
ifnull(lag(install_date) over (partition by universal_user_id,platform order by install_date desc),date('9999-12-31')) as next_install_date, 
from distinct_user_install_event
), 
full_user_date as ( 
select 
a.universal_user_id, 
a.install_date, 
a.platform, 
a.country, 
b.event_date 
from distinct_user_install_event_with_next a 
cross join event_window b 
where b.event_date >= a.install_date 
and b.event_date < a.next_install_date 
order by universal_user_id, event_date 
), 
data_flat_1 as ( 
select 
user_id as universal_user_id, 
event_name, 
parse_date("%Y%m%d",event_date)  as event_date, 
platform, 
if (event_name in ('login','first_open'), 1 , 0) as login_flag, 
 if (event_name in ('purchase') , 1 , 0) as pay_flag, 
if (event_name='click_ad',1,0) as click_ad__count,
if (event_name='login',1,0) as login__flag,
if (event_name='level_up',1,0) as level_up__count,
if (event_name='logout',1,0) as logout__count,
if (event_name='purchase', abstract_params(event_params,'total_price',4) ,null) as purchase__total_price,
if (event_name='first_open',1,0) as first_open__flag
from `allen-first.gaming_analytics.events` 
where parse_date("%Y%m%d",event_date)  >= date_sub(current_date(), interval 60 day) 
), 
event_agg_by_day as ( 
select 
universal_user_id,
event_date, 
platform, 
if(sum(login_flag)=0, 0,1) as login_flag, 
if(sum(pay_flag)=0, 0, 1 ) as pay_flag, 
ifnull(sum(case when event_name = 'click_ad' then click_ad__count end),0) as click_ad__count,
if(sum(case when event_name = 'login' then login__flag end)>0,1,0) as login__flag,
ifnull(sum(case when event_name = 'level_up' then level_up__count end),0) as level_up__count,
ifnull(sum(case when event_name = 'logout' then logout__count end),0) as logout__count,
ifnull(sum(case when event_name = 'purchase' then purchase__total_price end),0) as purchase__total_price,
if(sum(case when event_name = 'first_open' then first_open__flag end)>0,1,0) as first_open__flag
from data_flat_1
group by universal_user_id, event_date,platform 
) 
select 
a.*, 
ifnull(b.login_flag,0) as login_flag, 
ifnull(b.pay_flag,0) as pay_flag, 
 ifnull(b.click_ad__count,0) as click_ad__count,
ifnull(b.login__flag,0) as login__flag,
ifnull(b.level_up__count,0) as level_up__count,
ifnull(b.logout__count,0) as logout__count,
ifnull(b.purchase__total_price,0) as purchase__total_price,
ifnull(b.first_open__flag,0) as first_open__flag,
current_timestamp() as collect_time 
from full_user_date a 
left join event_agg_by_day b 
on a.universal_user_id=b.universal_user_id
and a.event_date=b.event_date 
and upper(a.platform)=upper(b.platform) 
