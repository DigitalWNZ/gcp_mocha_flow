--This script select distinct records from table_A which is the destination table in schedule_job_A 
--Step 1: Change xxxxx in sql below to table_A . 
--Step 2: Run and validate the query in bigquery 
--Step 3: Create a schedule job(aka:schedule_job_B) to write the  deduped result to table_B, save the name of table_B for usage in rolling_sum.sql 
with mocha_with_dup as ( 
select *, 
row_number() over (partition by universal_user_id,install_date, event_date,platform order by collect_time desc) as rn 
from `xxxxx` where event_date >= date_sub(current_date(),interval 60 day)) 
select * except (rn,collect_time) 
 from mocha_with_dup 
 where rn = 1 