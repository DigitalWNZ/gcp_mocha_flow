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
sum(_200056__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200056__count__sum,
sum(_100211__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100211__count__sum,
sum(_100062__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100062__count__sum,
sum(_200036__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200036__count__sum,
sum(_200190__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200190__count__sum,
_100577__flag,
_200179__flag,
_100023__flag,
_200079__flag,
_100537__flag,
_200051__flag,
_200218__flag,
_200157__flag,
_100031__flag,
sum(_100525__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100525__count__sum,
sum(_100097__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100097__count__sum,
_200177__flag,
_100194__flag,
_200050__flag,
_200122__flag,
_200114__flag,
sum(_100547__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100547__count__sum,
_200037__flag,
sum(_100186__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100186__count__sum,
_200121__flag,
sum(_100109__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100109__count__sum,
sum(_100521__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100521__count__sum,
_100566__flag,
_200032__flag,
_100099__flag,
sum(_100178__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100178__count__sum,
_100049__flag,
_100578__flag,
_200226__flag,
_100004__flag,
_200113__flag,
_200033__flag,
sum(_100156__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100156__count__sum,
_200204__flag,
sum(_100199__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100199__count__sum,
_200024__flag,
_100542__flag,
_200144__flag,
_200009__flag,
_200101__flag,
_100519__flag,
_100069__flag,
sum(_100574__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100574__count__sum,
_100122__flag,
_200105__flag,
_100131__flag,
sum(_100022__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100022__count__sum,
_100588__flag,
_200184__flag,
pay__pay_money,
sum(_100516__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100516__count__sum,
sum(_100041__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100041__count__sum,
_200008__flag,
_200108__flag,
_100217__flag,
_200064__flag,
_100539__flag,
sum(_100159__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100159__count__sum,
sum(_100101__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100101__count__sum,
sum(_100236__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100236__count__sum,
sum(_100080__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100080__count__sum,
_100193__flag,
_100116__flag,
sum(_100137__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100137__count__sum,
sum(_100514__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100514__count__sum,
_200147__flag,
_100226__flag,
sum(_100546__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100546__count__sum,
sum(_100138__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100138__count__sum,
sum(_100184__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100184__count__sum,
sum(_100013__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100013__count__sum,
sum(_100218__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100218__count__sum,
sum(_100153__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100153__count__sum,
_100020__flag,
sum(_100070__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100070__count__sum,
_100557__flag,
sum(_200176__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200176__count__sum,
sum(_100065__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100065__count__sum,
_200216__flag,
_200145__flag,
_200092__flag,
sum(_100212__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100212__count__sum,
sum(_100054__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100054__count__sum,
sum(_200030__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200030__count__sum,
_200158__flag,
_100202__flag,
_100161__flag,
_200016__flag,
_200149__flag,
sum(_100572__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100572__count__sum,
sum(_100162__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100162__count__sum,
sum(_100072__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100072__count__sum,
_200106__flag,
_200117__flag,
_200038__flag,
_200214__flag,
_200152__flag,
_200168__flag,
sum(_200007__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200007__count__sum,
sum(_200189__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200189__count__sum,
sum(_100569__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100569__count__sum,
sum(_100214__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100214__count__sum,
sum(_100012__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100012__count__sum,
sum(_100544__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100544__count__sum,
_100142__flag,
_200072__flag,
sum(_100148__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100148__count__sum,
sum(_100564__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100564__count__sum,
_200091__flag,
sum(_100043__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100043__count__sum,
sum(_200095__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200095__count__sum,
sum(_200151__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200151__count__sum,
sum(_100209__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100209__count__sum,
sum(_200097__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200097__count__sum,
sum(_200213__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200213__count__sum,
sum(_100034__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100034__count__sum,
sum(_200058__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200058__count__sum,
sum(_200155__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200155__count__sum,
sum(_100533__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100533__count__sum,
sum(_100589__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100589__count__sum,
_200019__flag,
sum(_200187__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200187__count__sum,
_200215__flag,
sum(_100575__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100575__count__sum,
sum(_200178__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200178__count__sum,
sum(_100026__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100026__count__sum,
sum(_200109__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200109__count__sum,
sum(_100548__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100548__count__sum,
_100117__flag,
sum(_100092__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100092__count__sum,
sum(_100170__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100170__count__sum,
sum(_100180__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100180__count__sum,
_100086__flag,
sum(_100106__item_number) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100106__item_number__sum,
_200055__flag,
sum(_200167__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200167__count__sum,
sum(_200220__item_number) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200220__item_number__sum,
_100213__flag,
sum(_100565__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100565__count__sum,
sum(_200219__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200219__count__sum,
sum(_200223__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200223__count__sum,
_100112__flag,
sum(_200217__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200217__count__sum,
sum(_100520__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100520__count__sum,
sum(_200115__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200115__count__sum,
sum(_200186__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200186__count__sum,
sum(_100029__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100029__count__sum,
sum(_200067__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200067__count__sum,
sum(_100540__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100540__count__sum,
sum(_100090__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100090__count__sum,
sum(_200002__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200002__count__sum,
sum(_100052__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100052__count__sum,
sum(_100061__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100061__count__sum,
_100017__flag,
_200169__flag,
sum(_100037__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100037__count__sum,
_100110__flag,
_100154__flag,
_200224__flag,
_100586__flag,
_200111__flag,
_200021__flag,
sum(_100536__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100536__count__sum,
sum(_100055__item_number) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100055__item_number__sum,
_200146__flag,
_100018__flag,
_100168__flag,
_100007__flag,
_200069__flag,
_100563__flag,
_100513__flag,
sum(_100060__item_number) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100060__item_number__sum,
_100130__flag,
sum(_100568__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100568__count__sum,
_100128__flag,
sum(_100024__item_number) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100024__item_number__sum,
_100064__flag,
_100538__flag,
sum(_100093__item_number) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100093__item_number__sum,
_200161__flag,
_100095__flag,
sum(_100166__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100166__count__sum,
sum(_200212__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200212__count__sum,
sum(gold__amount) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as gold__amount__sum,
_200013__flag,
_200100__flag,
_200004__flag,
sum(_100592__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100592__count__sum,
_100140__flag,
_100019__flag,
_100053__flag,
_100515__flag,
_200107__flag,
_100155__flag,
_100169__flag,
sum(_200077__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200077__count__sum,
sum(_200081__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200081__count__sum,
sum(_100063__item_number) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100063__item_number__sum,
_200010__flag,
sum(_100245__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100245__count__sum,
quit__flag,
_200006__flag,
_200063__flag,
_200046__flag,
_100145__flag,
sum(_100152__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100152__count__sum,
sum(_100238__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100238__count__sum,
sum(_100014__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100014__count__sum,
sum(_200191__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200191__count__sum,
_100051__flag,
_100144__flag,
_100143__flag,
sum(_200210__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200210__count__sum,
sum(_200045__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200045__count__sum,
_200089__flag,
sum(_100570__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100570__count__sum,
sum(_100554__item_number) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100554__item_number__sum,
_100543__flag,
sum(_100042__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100042__count__sum,
_200054__flag,
_100074__flag,
_200099__flag,
_200174__flag,
_200098__flag,
_100027__flag,
_100010__flag,
_200173__flag,
sum(_100030__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100030__count__sum,
_100120__flag,
_200082__flag,
_100146__flag,
sum(_100059__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100059__count__sum,
sum(_100111__item_number) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100111__item_number__sum,
_100118__flag,
_100006__flag,
_200003__flag,
_100021__flag,
_200088__flag,
_100104__flag,
_200022__flag,
sum(_100235__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100235__count__sum,
_200023__flag,
_100141__flag,
_200104__flag,
_100147__flag,
sum(_200048__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200048__count__sum,
_100040__flag,
_100139__flag,
sum(_100532__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100532__count__sum,
sum(_100203__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100203__count__sum,
sum(_200205__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200205__count__sum,
_100016__flag,
_100534__flag,
sum(_100207__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100207__count__sum,
login__flag,
_200020__flag,
_200093__flag,
_100195__flag,
sum(_100078__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100078__count__sum,
sum(_100015__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100015__count__sum,
sum(_100066__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100066__count__sum,
_100071__flag,
sum(_100567__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100567__count__sum,
sum(_200059__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200059__count__sum,
sum(_200029__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200029__count__sum,
_100158__flag,
sum(_200192__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200192__count__sum,
_200227__flag,
_200041__flag,
_100571__flag,
_100125__flag,
_200096__flag,
_100002__flag,
sum(_200017__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200017__count__sum,
_100048__flag,
_200188__flag,
sum(_100576__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _100576__count__sum,
_200015__flag,
sum(_200018__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as _200018__count__sum,
_100126__flag,
sum(role__count) over (partition by universal_user_id,platform,event_date order by event_date asc rows unbounded preceding) as role__count__sum
from `xxxxx` 
