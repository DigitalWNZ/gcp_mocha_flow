with event_window as (
select date('2020-04-09') as event_date union all
select date_sub(date('2020-04-09'), interval 1 day) union all
select date_sub(date('2020-04-09'), interval 2 day) union all
select date_sub(date('2020-04-09'), interval 3 day) union all
select date_sub(date('2020-04-09'), interval 4 day) union all
select date_sub(date('2020-04-09'), interval 5 day) union all
select date_sub(date('2020-04-09'), interval 6 day) union all
select date_sub(date('2020-04-09'), interval 7 day) union all
select date_sub(date('2020-04-09'), interval 8 day) union all
select date_sub(date('2020-04-09'), interval 9 day) union all
select date_sub(date('2020-04-09'), interval 10 day) union all
select date_sub(date('2020-04-09'), interval 11 day) union all
select date_sub(date('2020-04-09'), interval 12 day) union all
select date_sub(date('2020-04-09'), interval 13 day) union all
select date_sub(date('2020-04-09'), interval 14 day) union all
select date_sub(date('2020-04-09'), interval 15 day) union all
select date_sub(date('2020-04-09'), interval 16 day) union all
select date_sub(date('2020-04-09'), interval 17 day) union all
select date_sub(date('2020-04-09'), interval 18 day) union all
select date_sub(date('2020-04-09'), interval 19 day) union all
select date_sub(date('2020-04-09'), interval 20 day) union all
select date_sub(date('2020-04-09'), interval 21 day) union all
select date_sub(date('2020-04-09'), interval 22 day) union all
select date_sub(date('2020-04-09'), interval 23 day) union all
select date_sub(date('2020-04-09'), interval 24 day) union all
select date_sub(date('2020-04-09'), interval 25 day) union all
select date_sub(date('2020-04-09'), interval 26 day) union all
select date_sub(date('2020-04-09'), interval 27 day) union all
select date_sub(date('2020-04-09'), interval 28 day) union all
select date_sub(date('2020-04-09'), interval 29 day) union all
select date_sub(date('2020-04-09'), interval 30 day) union all
select date_sub(date('2020-04-09'), interval 31 day) union all
select date_sub(date('2020-04-09'), interval 32 day) union all
select date_sub(date('2020-04-09'), interval 33 day) union all
select date_sub(date('2020-04-09'), interval 34 day) union all
select date_sub(date('2020-04-09'), interval 35 day) union all
select date_sub(date('2020-04-09'), interval 36 day) union all
select date_sub(date('2020-04-09'), interval 37 day) union all
select date_sub(date('2020-04-09'), interval 38 day) union all
select date_sub(date('2020-04-09'), interval 39 day) union all
select date_sub(date('2020-04-09'), interval 40 day) union all
select date_sub(date('2020-04-09'), interval 41 day) union all
select date_sub(date('2020-04-09'), interval 42 day) union all
select date_sub(date('2020-04-09'), interval 43 day) union all
select date_sub(date('2020-04-09'), interval 44 day) union all
select date_sub(date('2020-04-09'), interval 45 day) union all
select date_sub(date('2020-04-09'), interval 46 day) union all
select date_sub(date('2020-04-09'), interval 47 day) union all
select date_sub(date('2020-04-09'), interval 48 day) union all
select date_sub(date('2020-04-09'), interval 49 day) union all
select date_sub(date('2020-04-09'), interval 50 day) union all
select date_sub(date('2020-04-09'), interval 51 day) union all
select date_sub(date('2020-04-09'), interval 52 day) union all
select date_sub(date('2020-04-09'), interval 53 day) union all
select date_sub(date('2020-04-09'), interval 54 day) union all
select date_sub(date('2020-04-09'), interval 55 day) union all
select date_sub(date('2020-04-09'), interval 56 day) union all
select date_sub(date('2020-04-09'), interval 57 day) union all
select date_sub(date('2020-04-09'), interval 58 day) union all
select date_sub(date('2020-04-09'), interval 59 day) union all
select date_sub(date('2020-04-09'), interval 60 day)
),
distinct_user_install_event as (
 select
distinct
device.advertising_id as universal_user_id,
parse_date("%Y%m%d",event_date) as install_date
from `allen-first.mocha_dataflow.sample_data`
where parse_date("%Y%m%d",event_date)  >= date_sub(date('2020-04-09'), interval 60 day)
and event_name= 'first_open'
),
distinct_user_install_event_with_next as (
select
universal_user_id,
install_date,
ifnull(lag(install_date) over (partition by universal_user_id order by install_date),date('9999-12-31')) as next_install_date
from distinct_user_install_event
),
full_user_date as (
select
a.universal_user_id,
a.install_date,
b.event_date
from distinct_user_install_event_with_next a
cross join event_window b
where b.event_date >=a.install_date
and b.event_date < a.next_install_date
order by universal_user_id, event_date
),
data_flat_1 as (
select
  Device.advertising_id AS universal_user_id,
  event_timestamp,
  event_name,
  event_date,
  if (event_name='in_app_purchase' and params.key = 'value',COALESCE(event_value_in_usd,params.value.int_value,params.value.float_value,params.value.double_value),null) as in_app_purchase__value,
  if (event_name='in_app_purchase' and params.key = 'currency', params.value.string_value,null) as in_app_purchase__currency,
  if (event_name='screen_view' and params.key='ga_session_number',params.value.int_value,null) as screen_view__ga_session_number,
  if (event_name='shop_page_viewed' and params.key='firebase_event_origin',1,null) as shop_page_viewed__flag,
from `allen-first.mocha_dataflow.sample_data`  ,unnest(event_params) as params
where event_name in ('in_app_purchase','screen_view','shop_page_viewed','ga_session_number') and Device.advertising_id is not null and Device.advertising_id != ""
and params.key in ('value','currency','ga_session_number','firebase_event_origin')
and parse_date("%Y%m%d",event_date) >= date_sub(date('2020-04-09'), interval 60 day)
),
data_flat_2 as (
select
universal_user_id,
event_timestamp,
event_name,
event_date,
max(in_app_purchase__value) as in_app_purchase__value,
max(in_app_purchase__currency) as in_app_purchase__currency,
max(screen_view__ga_session_number) as screen_view__ga_session_number,
max(shop_page_viewed__flag) as shop_page_viewed__flag
from data_flat_1
group by universal_user_id,event_name,event_date,event_timestamp
),
event_agg_by_day as (
select
universal_user_id,
event_date,
ifnull(sum(case when event_name='in_app_purchase' then in_app_purchase__value end),0) as in_app_purchase__value,
ifnull(sum(case when event_name='screen_view' then screen_view__ga_session_number end),0) as screen_view__ga_session_number,
if(sum(case when event_name='shop_page_viewed' then 1 end)>0,1,0) as shop_page_viewed__flag
from data_flat_2
group by universal_user_id,event_date
)
select
a.*,
ifnull(b.in_app_purchase__value,0) as in_app_purchase__value,
ifnull(b.screen_view__ga_session_number,0) as screen_view__ga_session_number,
ifnull(b.shop_page_viewed__flag,0) as in_app_purchase__value,
from full_user_date a
left join event_agg_by_day b
on a.universal_user_id=b.universal_user_id
and a.event_date=parse_date("%Y%m%d",b.event_date)
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
parse_date("%Y%m%d",event_date) as install_date
from `allen-first.mocha_dataflow.sample_data`
where parse_date("%Y%m%d",event_date) >= date_sub(current_date(), interval 60 day)
and event_name= 'first_open'
),
distinct_user_install_event_with_next as (
select
universal_user_id,
install_date,
ifnull(lag(install_date) over (partition by universal_user_id order by install_date),date('9999-12-31')) as next_install_date
from distinct_user_install_event
),
full_user_date as (
select
a.universal_user_id,
a.install_date,
b.event_date
from distinct_user_install_event_with_next a
cross join event_window b
where b.event_date >=a.install_date
and b.event_date < a.next_install_date
order by universal_user_id, event_date
),
data_flat_1 as (
select
  Device.advertising_id AS universal_user_id,
  event_timestamp,
  event_name,
  event_date,
  if (event_name='in_app_purchase' and params.key = 'value',COALESCE(event_value_in_usd,params.value.int_value,params.value.float_value,params.value.double_value),null) as in_app_purchase__value,
  if (event_name='in_app_purchase' and params.key = 'currency', params.value.string_value,null) as in_app_purchase__currency,
  if (event_name='screen_view' and params.key='ga_session_number',params.value.int_value,null) as screen_view__ga_session_number,
  if (event_name='shop_page_viewed' and params.key='firebase_event_origin',1,null) as shop_page_viewed__flag,
from `allen-first.mocha_dataflow.sample_data`  ,unnest(event_params) as params
where event_name in ('in_app_purchase','screen_view','shop_page_viewed','ga_session_number') and Device.advertising_id is not null and Device.advertising_id != ""
and params.key in ('value','currency','ga_session_number','firebase_event_origin')
and parse_date("%Y%m%d",event_date) >= date_sub(current_date(), interval 60 day)
),
data_flat_2 as (
select
universal_user_id,
event_timestamp,
event_name,
event_date,
max(in_app_purchase__value) as in_app_purchase__value,
max(in_app_purchase__currency) as in_app_purchase__currency,
max(screen_view__ga_session_number) as screen_view__ga_session_number,
max(shop_page_viewed__flag) as shop_page_viewed__flag
from data_flat_1
group by universal_user_id,event_name,event_date,event_timestamp
),
event_agg_by_day as (
select
universal_user_id,
event_date,
ifnull(sum(case when event_name='in_app_purchase' then in_app_purchase__value end),0) as in_app_purchase__value,
ifnull(sum(case when event_name='screen_view' then screen_view__ga_session_number end),0) as screen_view__ga_session_number,
if(sum(case when event_name='shop_page_viewed' then 1 end)>0,1,0) as shop_page_viewed__flag
from data_flat_2
group by universal_user_id,event_date
)
select
a.*,
ifnull(b.in_app_purchase__value,0) as in_app_purchase__value,
ifnull(b.screen_view__ga_session_number,0) as screen_view__ga_session_number,
ifnull(b.shop_page_viewed__flag,0) as in_app_purchase__value,
from full_user_date a
left join event_agg_by_day b
on a.universal_user_id=b.universal_user_id
and a.event_date=parse_date("%Y%m%d",b.event_date)



with mocha_with_dup as (select
 *,
 row_number() over (partition by uid,install_date,event_date order by collect_time desc) as rn
from `knight-us.mocha_bi.mocha_bi_user_events`),
mocha_remove_dup as (select
 * except(rn,collect_time)
from mocha_with_dup
where rn=1)


select
  device_id,
  install_date,
  event_date,
  sum_payment,
  pay_flag,
  login_flag,
  sum(area_war_fight) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as area_war_fight_sum,
  sum(repair_all) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as repair_all_sum,
  sum(user_levelup) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as user_levelup_sum,
  sum(build) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as build_sum,
  sum(train) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as train_sum,
  sum(get_online_reward) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as get_online_reward_sum,
  sum(world_attack_monster) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as world_attack_monster_sum,
  sum(adv_collect) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as adv_collect_sum,
  sum(unlock_tile) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as unlock_tile_sum,
  sum(march_gather_resource) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as march_gather_resource_sum,
  sum(merge_building) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as merge_building_sum,
  sum(complete_chapter) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as complete_chapter_sum,
  sum(upgrade_equip) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as upgrade_equip_sum,
  sum(marine_patrol_point) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as marine_patrol_point_sum,
  sum(activity_king_reward) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as activity_king_reward_sum,
  sum(first_to_world) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as first_to_world_sum,
  sum(machine_collect) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as machine_collect_sum,
  sum(hero_recruit_hero) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as hero_recruit_hero_sum,
  sum(alliance_gift_receive) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as alliance_gift_receive_sum,
  sum(activity_bp_reward) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as activity_bp_reward_sum,
  sum(machine_quick) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as machine_quick_sum,
  sum(panel_getitem) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as panel_getitem_sum,
  sum(open_treasure_box) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as open_treasure_box_sum,
  sum(exchange_buy_success) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as exchange_buy_success_sum,
  sum(science_levelup) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as science_levelup_sum,
  sum(get_new_treasure) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as get_new_treasure_sum,
  sum(hero_study_skill) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as hero_study_skill_sum,
  sum(adv_quickfight) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as adv_quickfight_sum,
  sum(money_tree) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as money_tree_sum,
  sum(power_rank_open) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as power_rank_open_sum,
  sum(panel_getcoin) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as panel_getcoin_sum,
  sum(goto_pay) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as goto_pay_sum,
  sum(alliance_move_apply) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as alliance_move_apply_sum,
  join_alliance,
  alliance_rally_monster,
  sum(alliance_shop_buy) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as alliance_shop_buy_sum,
  sum(alliance_science_donate) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as alliance_science_donate_sum,
  sum(machine_charge) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as machine_charge_sum,
  sum(server_id) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as server_id_sum,
  sum(hero_level_up) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as hero_level_up_sum,
  sum(alliance_rally_monster_fail) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as alliance_rally_monster_fail_sum,
  sum(hero_get) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as hero_get_sum,
  sum(alliance_reqhelp) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as alliance_reqhelp_sum,
  sum(hero_recruit_skill) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as hero_recruit_skill_sum,
  sum(quest_complete) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as quest_complete_sum,
  sum(merge_army) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as merge_army_sum,
  sum(save_defense_troops) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as save_defense_troops_sum,
  sum(rookie_start) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as rookie_start_sum,
  sum(install_equip) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as install_equip_sum,
  sum(attack_world_boss) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as attack_world_boss_sum,
  sum(alliance_help) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as alliance_help_sum,
  sum(complete_chapter_task) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as complete_chapter_task_sum,
  sum(repair_panel) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as repair_panel_sum,
  sum(rookie_end) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as rookie_end_sum,
  sum(adv_challenge) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as adv_challenge_sum,
  sum(alliance_move_invite) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as alliance_move_invite_sum,
  sum(warehouse_allin) over (partition by device_id,install_date,event_date order by event_date asc ROWS UNBOUNDED PRECEDING) as warehouse_allin_sum
from `knight-us.mocha_bi.mocha_bi_user_events_no_dup` ;
