import os
import json
import sys
import pandas as pd

def parse_config():

    with open('config.json.temp') as f:
        config_json=json.load(f)
        # print(config_json)
        return config_json

def gen_sql(config_json):
    event_window_table_name='event_window'
    distinct_user_install_event='distinct_user_install_event'
    distinct_user_install_event_with_next = 'distinct_user_install_event_with_next'
    full_user_date='full_user_date'
    data_flat_1='data_flat_1'
    data_flat_2 = 'data_flat_2'
    event_agg_by_day='event_agg_by_day'

    days_look_back=config_json['days_look_back']
    universal_user_id='universal_user_id'
    user_id='user_id'
    # Generate the full date list
    if config_json['start_date'] =='':
        date_function=config_json['date_function']
    else:
        date_function='date(\'' + config_json['start_date'] + '\')'

    # sql_str ='CREATE TEMP FUNCTION\n'  \
    #         +'abstract_params(input_arr Array<struct<key string, value struct<string_value string, int_value int64, float_value float64, double_value float64>>>, key_value string,value_field int64)\n' \
    #         + 'RETURNS int64\n' \
    #         + 'LANGUAGE js AS r"""\n' \
    #         + ' ret=0\n' \
    #         + ' var i=input_arr.length;\n' \
    #         + ' while(i--){\n' \
    #         + '     if (input_arr[i].key==key_value) {\n' \
    #         + '         if (value_field == 1) {\n' \
    #         + '             ret=input_arr[i].value.int_value;\n' \
    #         + '             i=0;\n' \
    #         + '         } else if (value_field == 2) {\n' \
    #         + '             ret=input_arr[i].value.float_value;\n' \
    #         + '             i=0\n' \
    #         + '         } else {\n'\
    #         + '             ret=input_arr[i].value.double_value;\n' \
    #         + '             i=0\n' \
    #         + '         }\n' \
    #         + '     }\n'\
    #         + ' };\n' \
    #         + ' if (ret === "") { \n' \
    #         + '     return 0; \n' \
    #         + ' } else { \n' \
    #         + '     return ret; \n' \
    #         + ' } \n' \
    #         + '""";\n'
    sql_str = 'CREATE TEMP FUNCTION\n' \
            + 'abstract_params(input_arr any type, key_value string,value_field int64) as ( \n' \
            + '(select\n' \
            + '  case\n' \
            + '     when value_field=1 then safe_cast(input_arr.value.string_value as float64)\n' \
            + '     when value_field=2 then safe_cast(input_arr.value.int_value as float64)\n' \
            + '     when value_field=3 then input_arr.value.float_value \n' \
            + '     else safe_cast(input_arr.value.double_value as float64)\n' \
            + '  end\n' \
            + 'from unnest(input_arr) as input_arr\n' \
            + 'where key=key_value\n' \
            + 'limit 1)\n' \
            + ');\n'

    sql_str=sql_str \
           + 'with ' + event_window_table_name + ' as ( \n'
    i=0
    while i <= days_look_back:
        if i == 0:
            sql_str += 'select ' + date_function + ' as event_date union all \n'
        elif i == days_look_back:
            sql_str += 'select date_sub(' + date_function + ', interval ' + str(i) + ' day) \n'
        else:
            sql_str += 'select date_sub(' + date_function + ', interval ' + str(i) + ' day) union all \n'
        i+=1
    sql_str += '), \n'
    # Generate distinct user install event
    if config_json['event_date_type'] == 'DATE':
        date_str = config_json['event_date_field']
    else:
        if config_json['event_date_format']=='YYYYMMDD':
            date_str= 'parse_date("%Y%m%d",' + config_json['event_date_field'] + ') '
        else:
            date_str='date(' + config_json['event_date_field'] + ') '

    # sql_str = sql_str \
    #         + distinct_user_install_event +' as ( \nselect \ndistinct \n' \
    #         + config_json[universal_user_id] + ' as ' +  universal_user_id + ', \n' \
    #         + date_str + ' as install_date \n' \
    #         + 'from `' + config_json['table_name'].replace('`','')+ '` \n' \
    #         + 'where ' + date_str + ' >= date_sub(' + date_function+ ', interval ' + str(days_look_back) + ' day) \n' \
    #         + 'and ' + config_json['event_name_field'] + '= \'' + config_json['install_event'] + '\' \n' \
    #         + '), \n'

    sql_str = sql_str \
            + distinct_user_install_event +' as ( \nselect \n' \
            + config_json[universal_user_id] + ' as ' +  universal_user_id + ', \n' \
            + date_str + ' as install_date, \n' \
            + 'platform, \n' \
            + 'geo.country, \n' \
            + 'count(0) \n' \
            + 'from `' + config_json['table_name'].replace('`','')+ '` \n' \
            + 'where ' + date_str + ' >= date_sub(' + date_function+ ', interval ' + str(days_look_back) + ' day) \n' \
            + 'and ' + config_json['event_name_field'] + '= \'' + config_json['install_event'] + '\' \n' \
            + 'group by 1,2,3,4), \n'
    # Generate distinct user install event with next
    sql_str = sql_str \
            + distinct_user_install_event_with_next + ' as ( \n' \
            + 'select \n' \
            + universal_user_id + ',' \
            + 'install_date, platform, country,\n' \
            + 'ifnull(lag(install_date) over (partition by universal_user_id,platform order by install_date desc),date(\'9999-12-31\')) as next_install_date, \n' \
            + 'from ' + distinct_user_install_event + '\n' \
            + '), \n'
    # Cross join user_date and full_event_window
    sql_str = sql_str \
            + full_user_date + ' as ( \n' \
            + 'select \n' \
            + 'a.' + universal_user_id +', \n' \
            + 'a.install_date, \n' \
            + 'a.platform, \n' \
            + 'a.country, \n' \
            + 'b.event_date \n' \
            + 'from ' + distinct_user_install_event_with_next + ' a \n' \
            + 'cross join ' + event_window_table_name + ' b \n' \
            + 'where b.event_date >= a.install_date \n' \
            + 'and b.event_date < a.next_install_date \n' \
            + 'order by ' + universal_user_id + ', event_date \n' \
            + '), \n'

    # flat event_date
    list_event_agg=config_json['event_aggregation']
    df_event_agg=pd.DataFrame.from_records(list_event_agg)
    list_agg_event_name= df_event_agg['event_name'].tolist()
    list_agg_event_name=[x.replace('.','_') for x in list_agg_event_name]
    event_name_str= '('
    for x in list_agg_event_name:
        event_name_str = event_name_str + '\''+ x + '\','
    event_name_str=event_name_str[:-1] + ')'
    print(event_name_str)

    list_agg_agg=df_event_agg['Aggregation'].tolist()
    list_agg_event_value=df_event_agg['Event_value'].tolist()
    # unnest=False
    for field in list_agg_event_value:
        if type(field) is dict:
            # unnest=True
            event_value=field['value_field']
            if event_value.find('.') == -1:
                raise ValueError('field {} is not valid field in repeated column field '.format(event_value))
    sql_str = sql_str \
            + data_flat_1 + ' as ( \n' \
            + 'select \n' \
            + config_json[universal_user_id] + ' as ' +  universal_user_id + ', \n' \
            + 'event_name, \n' \
            + date_str + ' as event_date, \n' \
            + 'platform, \n'
            # + 'event_timestamp, \n' \

    pay_events=config_json['pay_events']

    pay_event_in_aggregation=False
    for pay_event in pay_events:
        if pay_event in list_agg_event_name:
            pay_event_in_aggregation=True
            break
    if not pay_event_in_aggregation:
        raise ValueError('No aggregation is defined for any payment events')

    login_events=config_json['login_events']
    str_login='(\''
    for login in login_events:
        str_login = str_login \
                  + login + '\','
    # str_login=str_login[:-1] + ')'
    str_login=str_login[:-1] + ',\'' + config_json['install_event'] + '\')'
    str_pay='(\''
    for pay in pay_events:
        str_pay = str_pay \
                  + pay + '\','
    str_pay=str_pay[:-1] + ')'
    sql_str = sql_str \
            + 'if (event_name in ' + str_login + ', 1 , 0) as login_flag, \n'\
            +' if (event_name in ' + str_pay + ' , 1 , 0) as pay_flag, \n'
    event_agg_str=''
    list_alias=[]
    key_value_str='('
    for i in range(len(list_agg_event_name)):
        event_name=list_agg_event_name[i]
        event_agg=list_agg_agg[i]
        event_value=list_agg_event_value[i]

        if event_agg=='sum':
            if type(event_value) is not dict:
                alias=event_name + '__' + event_value
                list_alias.append(alias)
                event_agg_str = event_agg_str \
                              + 'if (event_name=\'' + event_name + '\',' + event_value + ',null) as ' + alias + ',\n'
            else:
                event_value_key_field=event_value['key_field']
                event_value_key_value=event_value['key_value']
                key_value_str=key_value_str + '\'' + event_value_key_value + '\','
                event_value_value=event_value['value_field']
                if event_value_value == 'event_params.value.string_value':
                    event_value_value=1
                elif event_value_value == 'event_params.value.int_value':
                    event_value_value=2
                elif event_value_value == 'event_params.value.float_value':
                    event_value_value=3
                else:
                    event_value_value = 4
                alias=event_name + '__'+ event_value_key_value
                list_alias.append(alias)
                event_agg_str = event_agg_str \
                                + 'if (event_name=\'' + event_name + '\', abstract_params(event_params,\'' + event_value_key_value + '\',' + str(event_value_value) +  ') ,null) as ' +alias+ ',\n'
        elif event_agg == 'count':
            alias = event_name + '__count'
            list_alias.append(alias)
            event_agg_str = event_agg_str \
                            + 'if (event_name=\'' + event_name + '\',1,0) as ' + alias + ',\n'
        elif event_agg == 'flag':
            alias = event_name + '__flag'
            list_alias.append(alias)
            event_agg_str = event_agg_str \
                            + 'if (event_name=\'' + event_name + '\',1,0) as ' + alias + ',\n'
        else:
            raise ValueError('The aggregation {} is not supported')
    key_value_str=key_value_str[:-1] + ')'
    print(key_value_str)
    # if unnest:
    #     table_str = 'from `' + config_json['table_name'].replace('`','') + '` ,unnest(event_params) as event_params \n' \
    #               + 'where ' + date_str + ' >= date_sub(' + date_function+ ', interval ' + str(days_look_back) + ' day) \n' \
    #               + 'and event_name in' + event_name_str + '\n'
    #               # + 'and event_params.key in' + key_value_str +'\n'
    # else:
    table_str = 'from `' + config_json['table_name'].replace('`','') + '` \n' \
              + 'where ' + date_str + ' >= date_sub(' + date_function+ ', interval ' + str(days_look_back) + ' day) \n'
              # + 'and event_name in' + event_name_str + '\n'
    # print(event_agg_str[:-3])
    sql_str = sql_str \
            + event_agg_str[:-2] + '\n' \
            + table_str \
            + '), \n'
    # group flat data
    # if unnest:
    #     sql_str = sql_str \
    #             + data_flat_2 + ' as ( \n' \
    #             + 'select \n' \
    #             + universal_user_id + ', \n' \
    #             + 'event_name, \n' \
    #             + 'event_timestamp, \n' \
    #             + 'event_date, \n' \
    #             + 'max(login_flag) as login_flag, \n' \
    #             + 'max(pay_flag) as pay_flag, \n'
    #
    #     event_agg_str=''
    #     for i in range(len(list_agg_event_name)):
    #         event_agg_str = event_agg_str \
    #                       + 'max(' + list_alias[i] + ') as ' + list_alias[i] + ', \n'
    #     sql_str = sql_str \
    #             + event_agg_str[:-2] + '\n'   \
    #             + 'from ' + data_flat_1 + '\n' \
    #             + 'group by ' + universal_user_id + ', event_name, event_date,event_timestamp \n' \
    #             + '), \n'

    # event agg by day
    sql_str = sql_str \
            + event_agg_by_day+ ' as ( \n' \
            + 'select \n' \
            + universal_user_id + ',\n' \
            + 'event_date, \n' \
            + 'platform, \n' \
            + 'if(sum(login_flag)=0, 0,1) as login_flag, \n' \
            + 'if(sum(pay_flag)=0, 0, 1 ) as pay_flag, \n'

    event_agg_str = ''
    for i in range(len(list_agg_event_name)):
        event_name=list_agg_event_name[i]
        event_agg=list_agg_agg[i]
        event_value=list_agg_event_value[i]
        if event_agg=='sum':
            event_agg_str = event_agg_str \
                          + 'ifnull(sum(case when event_name = ' + '\'' + event_name + '\' then ' + list_alias[i] + ' end),0) as ' + list_alias[i] + ',\n'
        elif event_agg == 'count':
            event_agg_str = event_agg_str \
                            + 'ifnull(sum(case when event_name = ' + '\'' + event_name + '\' then ' + list_alias[i] + ' end),0) as ' + list_alias[i] + ',\n'
        else:
            event_agg_str = event_agg_str \
                           + 'if(sum(case when event_name = ' + '\'' + event_name + '\' then ' + list_alias[i] + ' end)>0,1,0) as ' + list_alias[i] + ',\n'
    table_str='from '
    # if unnest:
    #     table_str = table_str \
    #               + data_flat_2 + '\n' \
    #               + 'group by ' + universal_user_id + ' ,event_date \n' \
    #               + ') \n'
    # else:
    table_str = table_str \
              + data_flat_1 + '\n' \
              + 'group by ' + universal_user_id + ', event_date,platform \n' \
              + ') \n'

    sql_str = sql_str \
            + event_agg_str[:-2] + '\n'  \
            + table_str

    # join with full_user_day
    sql_str = sql_str \
            + 'select \n' \
            + 'a.*, \n' \
            + 'ifnull(b.login_flag,0) as login_flag, \n' \
            + 'ifnull(b.pay_flag,0) as pay_flag, \n '
    event_agg_str=''
    for i in range(len(list_agg_event_name)):
        event_agg_str = event_agg_str \
                      + 'ifnull(b.' +  list_alias[i] + ',0) as ' + list_alias[i] + ',\n'

    # print(event_agg_str[:-2])
    sql_str = sql_str \
            + event_agg_str[:-2] + ',\n'  \
            + 'current_timestamp() as collect_time \n' \
            + 'from ' + full_user_date + ' a \n' \
            + 'left join ' + event_agg_by_day + ' b \n' \
            + 'on a.' + universal_user_id + '=b.' + universal_user_id + '\n' \
            + 'and a.event_date=b.event_date \n' \
            + 'and upper(a.platform)=upper(b.platform) \n'
    print('-----------------Daily transform-----------------')
    print(sql_str)
    comment_str='--This script aggregate by day the events from users, who installed our apps in last N days.  \n' \
               +'--Step 1: Run and validate the query below in bigquery \n' \
               +'--Step 2: Create a daily schedule job(aka: schedule_job_A) to write the query result to table_A which contains results from all execution of the schedule job \n ' \
               +'--so their will be duplicate records in table A, the dedup script is in dedup.sql. Save the name of table_A for usage in dedup.sql \n'

    with open("daily_agg.sql", "w") as text_file:
        text_file.write(comment_str + sql_str)

    # dedup
    comment_str='--This script select distinct records from table_A which is the destination table in schedule_job_A \n'\
               +'--Step 1: Change xxxxx in sql below to table_A . \n' \
               +'--Step 2: Run and validate the query in bigquery \n' \
               +'--Step 3: Create a schedule job(aka:schedule_job_B) to write the  deduped result to table_B, save the name of table_B for usage in rolling_sum.sql \n'

    dedup_sql_str = 'with mocha_with_dup as ( \n' \
                  + 'select *, \n' \
                  + 'row_number() over (partition by ' + universal_user_id + ',install_date, event_date,platform order by collect_time desc) as rn \n' \
                  + 'from `xxxxx`) \n' \
                  + 'select * except (rn,collect_time) \n ' \
                  + 'from mocha_with_dup \n ' \
                  + 'where rn = 1 '
    # dedup_sql_str= 'with daily_agg as ( \n ' \
    #               + 'select ' + universal_user_id + ',' + user_id + ',install_date,event_date, \n' \
    #               +'array_agg(src order by ' + universal_user_id + ',' + user_id + ',install_date,event_date desc limit 1)[offset(0)].* except(' \
    #               + universal_user_id + ',' + user_id + ',install_date,event_date) \n' \
    #               + 'from `xxxxx` src group by 1,2,3,4) \n' \
    #               + 'select * from daily_agg'

    # dedup_sql_str= 'select ' + universal_user_id + ',' + user_id + ',install_date,event_date, \n' \
    #               +'array_agg(src order by ' + universal_user_id +',' + user_id + ',install_date,event_date desc limit 1)[offset(0)].* except(' \
    #               + universal_user_id + ',' + user_id + ',install_date,event_date) \n' \
    #               + 'from `xxxxx` src group by 1,2,3,4'

    with open("dedup.sql", "w") as text_file:
        text_file.write(comment_str + dedup_sql_str)
    print('-----------------Daily dedup transform-----------------')
    print(dedup_sql_str)
    # sum as of today
    comment_str='--This script calcualte the rolling_sum as of event_date for each event of each customer based on table_B which is the destination table in schedule_job_B. \n'\
               +'--Step 1: Change xxxxx in sql below to table_B. \n' \
               +'--Step 2: Run and validate the query in bigquery \n' \
               +'--Step 3: Create a schedule job(aka:schedule_job_C) to write the query  result to table_C.\n'

    sum_sql_str='select \n' \
               + universal_user_id + ',\n' \
               + 'install_date,\n' \
               + 'event_date,\n' \
               + 'login_flag, \n' \
               + 'pay_flag, \n' \
               + 'platform, \n' \
               + 'country, \n'

    event_agg_str=''
    for i in range(len(list_alias)):
        event_agg=list_agg_agg[i]
        event_name=list_agg_event_name[i]
        if event_agg != 'flag' and event_name not in pay_events:
            event_agg_str = event_agg_str \
                        + 'sum(' + list_alias[i] + ') over (partition by ' + universal_user_id + ',platform,event_date order by event_date asc rows unbounded preceding) as ' + list_alias[i]+'__sum,\n'
        else:
            event_agg_str = event_agg_str \
                          + list_alias[i]+ ',\n'

    sum_sql_str = sum_sql_str \
                + event_agg_str[:-2] + '\n' \
                + 'from `xxxxx` \n'
                # + 'order by ' + universal_user_id + ', event_date \n'
    print('-----------------Rolling sum transform-----------------')
    print(sum_sql_str)
    with open("rolling_sum.sql", "w") as text_file:
        text_file.write(comment_str + sum_sql_str)




if __name__ == '__main__':
    config_json=parse_config()
    sql_str=gen_sql(config_json)
