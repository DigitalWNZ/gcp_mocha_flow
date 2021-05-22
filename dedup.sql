--This script select distinct records from table_A which is the destination table in schedule_job_A 
--Step 1: Change xxxxx in sql below to table_A . 
--Step 2: Run and validate the query in bigquery 
--Step 3: Create a schedule job(aka:schedule_job_B) to write the  deduped result to table_B, save the name of table_B for usage in rolling_sum.sql 
with daily_agg as ( 
 select universal_user_id,install_date,event_date, 
array_agg(src order by universal_user_id,install_date,event_date desc limit 1)[offset(0)].* except(universal_user_id,install_date,event_date) 
from `xxxxx` src group by 1,2,3), 
select * from daily_agg