drop table if exists train_count_fea;
CREATE TABLE train_count_fea as
select event_id
		--change -1 to 1
        ,CAST(case when is_fraud = 0 then 0 else 1 end as BIGINT ) as label
                
		--one hot
		,a.network
        ,a.mobile_oper_platform
        ,a.pay_scene
        ,a.operation_channel
       	,a.gmt_occur
        
        --counting features
        ,user_count_in_hour
        ,ip_count_in_hour
        ,device_count_in_hour
        ,pay_sence_count_in_hour
        ,operation_channel_count_in_hour
        ,network_count_in_hour
        ,mobile_oper_platform_count_in_hour
        ,mobile_oper_platform_user_count_in_hour
        ,card_cert_prov_city_user_count_in_hour
        ,card_mobile_prov_city_user_count_in_hour
        ,card_bin_prov_city_user_count_in_hour
        ,cert_prov_city_user_count_in_hour
        ,oppo_user_count_in_hour        
        ,oppo_card_count_in_hour
        ,user_oppo_device_count_in_hour
        ,user_income_card_no_count_in_hour
        ,oppo_income_card_no_count_in_hour
        ,user_plat_chan_count_in_hour
		,user_plat_scene_count_in_hour
		,user_scene_chan_count_in_hour
		,user_plat_net_count_in_hour
		,user_scene_net_count_in_hour
		,user_chan_net_count_in_hour
		,user_ip_net_count_in_hour
		,user_ip_chan_count_in_hour
		,user_ip_scene_count_in_hour
		,user_ip_plat_count_in_hour
		,user_dev_plat_count_in_hour
		,user_dev_net_count_in_hour
		,user_dev_ip_count_in_hour
		,user_dev_oper_count_in_hour
		,user_dev_scene_count_in_hour
        ,oppo_count_in_hour
        ,network_ip_count
		,oper_scene_platform_count
        ,oper_scene_network_count
        
   
		--amt features
        ,amt_sum_in_hour
        ,avg(amt) over(partition by a.user_id  order by a.gmt_occur) as avg_trade_amt_before_cur_user
        ,sum(amt) over(partition by a.opposing_id  order by a.gmt_occur) as before_sum_oppo_id_sum_amt
		,sum(amt) over(partition by a.user_id order by a.gmt_occur) as before_sum_user_id_sum_amt

        --accumulate features
        ,sum(user_count_in_hour) over(PARTITION by a.user_id ORDER by a.gmt_occur) as user_total_trade_time_till_cnt_hour
       	,sum(oppo_count_in_hour) over(PARTITION by a.opposing_id ORDER by a.gmt_occur) as oppo_total_trade_time_till_cnt_hour
		,sum(oppo_user_count_in_hour) over(PARTITION by a.user_id,a.opposing_id ORDER by a.gmt_occur) as oppo_user_total_trade_time_till_cnt_hour
		,sum(device_count_in_hour) over(PARTITION by a.user_id,a.device_sign ORDER by a.gmt_occur) as user_device_total_trade_time_till_cnt_hour
		,sum(operation_channel_count_in_hour) over(PARTITION by a.user_id,a.operation_channel ORDER by a.gmt_occur) as user_oper_chan_total_trade_time_till_cnt_hour
		,sum(ip_count_in_hour) over(PARTITION by a.user_id,a.client_ip ORDER by a.gmt_occur) as user_ip_total_trade_time_till_cnt_hour
		,sum(network_count_in_hour) over(PARTITION by a.user_id,a.network ORDER by a.gmt_occur) as user_network_total_trade_time_till_cnt_hour
        ,sum(pay_sence_count_in_hour) over(PARTITION by a.user_id,a.pay_scene ORDER by a.gmt_occur) as user_pay_scene_total_trade_time_till_cnt_hour
		,sum(mobile_oper_platform_count_in_hour) over(PARTITION by a.user_id,a.mobile_oper_platform ORDER by a.gmt_occur) as user_mobile_platform_total_trade_time_till_cnt_hour

		--time features
        ,unix_timestamp(to_date(a.gmt_occur, 'yyyy-mm-dd hh')) - lag(unix_timestamp(to_date(a.gmt_occur, 'yyyy-mm-dd hh'))) over(partition by a.user_id order by a.gmt_occur asc) as difftime_last_trade
		,unix_timestamp(to_date(a.gmt_occur, 'yyyy-mm-dd hh')) - lag(unix_timestamp(to_date(a.gmt_occur, 'yyyy-mm-dd hh')),2) over(partition by a.user_id order by a.gmt_occur asc) as difftime_last_last_trade
		,datepart(to_date(a.gmt_occur, 'yyyy-mm-dd hh'),'dd') as day
		,datepart(to_date(a.gmt_occur, 'yyyy-mm-dd hh'),'hh') as hour

		--rank features
        ,rank() over(partition by a.user_id,a.card_bin_prov,a.card_mobile_prov,a.card_cert_prov,a.ip_prov,a.cert_prov order by a.gmt_occur) as province_cumcount_rank
        ,rank() over(partition by a.user_id,a.card_bin_prov,a.card_mobile_prov,a.card_cert_prov,a.ip_prov,a.cert_prov,a.card_bin_city,a.card_mobile_city,a.card_cert_city,a.ip_city,a.cert_city order by a.gmt_occur) as province_city_cumcount_rank
        ,rank() over(partition by a.user_id,a.card_bin_city,a.card_mobile_city,a.card_cert_city,a.ip_city,a.cert_city order by a.gmt_occur) as city_cumcount_rank
        ,rank() over(partition by a.user_id,a.card_bin_prov,a.card_mobile_prov,a.card_cert_prov,a.ip_prov,a.cert_prov,a.province order by a.gmt_occur) / rank() over(partition by a.user_id  order by a.gmt_occur) as province_cumcount_rank_per
		,rank() over(partition by a.user_id,a.client_ip,a.ip_prov,a.ip_city order by a.gmt_occur) as ip_prov_city_cumcount
		,rank() over(partition by a.user_id  order by a.gmt_occur) as before_cnt_user_id
        ,rank() over(partition by a.user_id,a.client_ip  order by a.gmt_occur) as before_cnt_user_id_same_client_ip
        ,rank() over(partition by a.user_id,a.network  order by a.gmt_occur) as before_cnt_user_id_same_net
        ,rank() over(partition by a.user_id,a.operation_channel  order by a.gmt_occur) as before_cnt_user_id_same_chan
        ,rank() over(partition by a.user_id,a.device_sign  order by a.gmt_occur) as before_cnt_user_id_same_device_sign
        ,rank() over(partition by a.user_id,a.mobile_oper_platform  order by a.gmt_occur) as before_cnt_user_id_same_oper
        ,rank() over(partition by a.user_id,a.pay_scene  order by a.gmt_occur) as before_cnt_user_id_same_scene
		,rank() over(partition by a.user_id,a.client_ip  order by a.gmt_occur)/rank() over(partition by a.user_id  order by a.gmt_occur) as before_cnt_user_id_same_client_ip_per
		,rank() over(partition by a.user_id,a.device_sign  order by a.gmt_occur)/rank() over(partition by a.user_id  order by a.gmt_occur) as before_cnt_user_id_same_device_sign_per
        ,rank() over(partition by a.user_id,a.amt  order by a.gmt_occur) as before_cnt_user_id_same_amt
        ,rank() over(partition by a.user_id,a.amt  order by a.gmt_occur)/rank() over(partition by a.user_id  order by a.gmt_occur) as before_cnt_user_id_same_amt_per
        ,rank() over(partition by a.opposing_id,a.card_cert_no,a.income_card_no  order by a.gmt_occur) as oppo_card_income_cumcount
        ,rank() over(partition by a.user_id,a.device_sign,a.mobile_oper_platform,a.operation_channel  order by a.gmt_occur) as before_cnt_user_id_same_device_sign_mobile_oper_channel_rank
		,rank() over(partition by a.opposing_id,a.user_id order by a.gmt_occur) as user_oppo_rank
        ,rank() over(partition by a.opposing_id,a.pay_scene order by a.gmt_occur) as oppo_scene_rank
        ,rank() over(partition by a.opposing_id,a.operation_channel order by a.gmt_occur) as oppo_operation_channel_rank
        ,rank() over(partition by a.opposing_id,a.mobile_oper_platform order by a.gmt_occur) as oppo_mobile_oper_platform_rank
        ,rank() over(partition by a.opposing_id,a.card_cert_no order by a.gmt_occur) as oppo_card_cert_no_rank
        ,rank() over(partition by a.opposing_id,a.device_sign order by a.gmt_occur) as oppo_device_sign_rank
        ,rank() over(partition by a.opposing_id,a.user_id,a.pay_scene order by a.gmt_occur) as user_oppo_scene_rank
        ,rank() over(partition by a.opposing_id,a.user_id,a.operation_channel order by a.gmt_occur) as user_oppo_operation_channel_rank
        ,rank() over(partition by a.opposing_id,a.user_id,a.mobile_oper_platform order by a.gmt_occur) as user_oppo_mobile_oper_platform_rank
        ,rank() over(partition by a.opposing_id,a.user_id,a.card_cert_no order by a.gmt_occur) as user_oppo_card_cert_no_rank
        ,rank() over(partition by a.opposing_id,a.user_id,a.device_sign order by a.gmt_occur) as user_oppo_device_sign_rank
        ,rank() over(partition by a.user_id,a.client_ip,a.operation_channel  order by a.gmt_occur) as user_ip_oper_rank
        ,rank() over(partition by a.user_id,a.client_ip,a.mobile_oper_platform  order by a.gmt_occur) as user_ip_plat_rank
        ,rank() over(partition by a.user_id,a.client_ip,a.network  order by a.gmt_occur) as user_ip_net_rank
        ,rank() over(partition by a.user_id,a.client_ip,a.pay_scene order by a.gmt_occur) as user_ip_scene_rank
        ,rank() over(partition by a.user_id,a.client_ip,a.device_sign order by a.gmt_occur) as user_ip_dev_rank
        ,rank() over(partition by a.user_id,a.network,a.operation_channel  order by a.gmt_occur) as user_net_oper_rank
        ,rank() over(partition by a.user_id,a.network,a.mobile_oper_platform  order by a.gmt_occur) as user_net_plat_rank
        ,rank() over(partition by a.user_id,a.network,a.pay_scene order by a.gmt_occur) as user_net_scene_rank
        ,rank() over(partition by a.user_id,a.network,a.device_sign order by a.gmt_occur) as user_net_dev_rank
        ,rank() over(partition by a.user_id,a.mobile_oper_platform,a.operation_channel  order by a.gmt_occur) as user_oper_chan_rank
        ,rank() over(partition by a.user_id,a.mobile_oper_platform,a.pay_scene order by a.gmt_occur) as user_oper_scene_rank
        ,rank() over(partition by a.user_id,a.mobile_oper_platform,a.device_sign order by a.gmt_occur) as user_oper_dev_rank
        ,rank() over(partition by a.user_id,a.device_sign,a.pay_scene order by a.gmt_occur) as user_dev_scene_rank
        ,rank() over(partition by a.user_id,a.device_sign,a.operation_channel order by a.gmt_occur) as user_chan_dev_rank
        ,rank() over(partition by a.user_id,a.pay_scene,a.operation_channel order by a.gmt_occur) as user_scene_oper_rank
        
        --dense rank features
        ,dense_rank() over(partition by a.opposing_id,a.user_id order by a.gmt_occur) as user_oppo_dense_rank
        ,dense_rank() over(partition by a.opposing_id,a.pay_scene order by a.gmt_occur) as oppo_scene_dense_rank
        ,dense_rank() over(partition by a.opposing_id,a.operation_channel order by a.gmt_occur) as oppo_operation_channel_dense_rank
        ,dense_rank() over(partition by a.opposing_id,a.mobile_oper_platform order by a.gmt_occur) as oppo_mobile_oper_platform_dense_rank
        ,dense_rank() over(partition by a.opposing_id,a.card_cert_no order by a.gmt_occur) as oppo_card_cert_no_dense_rank
        ,dense_rank() over(partition by a.opposing_id,a.device_sign order by a.gmt_occur) as oppo_device_sign_dense_rank
        ,dense_rank() over(partition by a.opposing_id,a.user_id,a.pay_scene order by a.gmt_occur) as user_oppo_scene_dense_rank
        ,dense_rank() over(partition by a.opposing_id,a.user_id,a.operation_channel order by a.gmt_occur) as user_oppo_operation_channel_dense_rank
        ,dense_rank() over(partition by a.opposing_id,a.user_id,a.card_cert_no order by a.gmt_occur) as user_oppo_card_cert_no_dense_rank
        ,dense_rank() over(partition by a.opposing_id,a.user_id,a.device_sign order by a.gmt_occur) as user_oppo_device_sign_dense_rank
        ,dense_rank() over(partition by a.user_id,a.card_bin_prov,a.card_mobile_prov,a.card_cert_prov,a.ip_prov,a.cert_prov,a.card_bin_city,a.card_mobile_city,a.card_cert_city,a.ip_city,a.cert_city order by a.gmt_occur) as province_city_cumcount_dense_rank
        ,dense_rank() over(partition by a.user_id,a.card_bin_city,a.card_mobile_city,a.card_cert_city,a.ip_city,a.cert_city order by a.gmt_occur) as city_cumcount_dense_rank
        ,dense_rank() over(partition by a.user_id,a.card_bin_prov,a.card_mobile_prov,a.card_cert_prov,a.ip_prov,a.cert_prov,a.province order by a.gmt_occur) as province_cumcount_dense_rank
        ,dense_rank() over(partition by a.user_id,a.device_sign,a.mobile_oper_platform,a.operation_channel  order by a.gmt_occur) as before_cnt_user_id_same_device_sign_mobile_oper_channel_dense_rank
        ,dense_rank() over(partition by a.user_id,a.client_ip  order by a.gmt_occur) as before_cnt_user_id_same_client_ip_dense_rank
        ,dense_rank() over(partition by a.user_id,a.device_sign  order by a.gmt_occur) as before_cnt_user_id_same_device_sign_dense_rank
        ,dense_rank() over(partition by a.user_id,a.client_ip  order by substr(cast(a.gmt_occur as string),1,10)) as before_cnt_user_id_same_client_ip_dense_rank_2
        ,dense_rank() over(partition by a.user_id,a.device_sign  order by substr(cast(a.gmt_occur as string),1,10)) as before_cnt_user_id_same_device_sign_dense_rank_2
        ,dense_rank() over(partition by a.user_id,a.operation_channel  order by a.gmt_occur) as before_cnt_user_id_same_operation_channel_dense_rank
		,dense_rank() over(partition by a.user_id,a.pay_scene  order by a.gmt_occur) as before_cnt_user_id_same_pay_scene_dense_rank
		,dense_rank() over(partition by a.user_id,a.mobile_oper_platform  order by a.gmt_occur) as before_cnt_user_id_same_mobile_oper_platform_dense_rank
        ,dense_rank() over(partition by a.user_id,a.amt  order by substr(cast(a.gmt_occur as string),1,10)) as before_cnt_user_id_same_amt_dense_rank_2
        ,dense_rank() over(partition by a.user_id,a.client_ip,a.operation_channel  order by a.gmt_occur) as user_ip_oper_dense_rank
        ,dense_rank() over(partition by a.user_id,a.client_ip,a.mobile_oper_platform  order by a.gmt_occur) as user_ip_plat_dense_rank
        ,dense_rank() over(partition by a.user_id,a.client_ip,a.network  order by a.gmt_occur) as user_ip_net_dense_rank
        ,dense_rank() over(partition by a.user_id,a.client_ip,a.pay_scene order by a.gmt_occur) as user_ip_scene_dense_rank
        ,dense_rank() over(partition by a.user_id,a.client_ip,a.device_sign order by a.gmt_occur) as user_ip_dev_dense_rank
        ,dense_rank() over(partition by a.user_id,a.network,a.operation_channel  order by a.gmt_occur) as user_net_oper_dense_rank
        ,dense_rank() over(partition by a.user_id,a.network,a.mobile_oper_platform  order by a.gmt_occur) as user_net_plat_dense_rank
        ,dense_rank() over(partition by a.user_id,a.network,a.pay_scene order by a.gmt_occur) as user_net_scene_dense_rank
        ,dense_rank() over(partition by a.user_id,a.network,a.device_sign order by a.gmt_occur) as user_net_dev_dense_rank
        ,dense_rank() over(partition by a.user_id,a.mobile_oper_platform,a.operation_channel  order by a.gmt_occur) as user_oper_chan_dense_rank
        ,dense_rank() over(partition by a.user_id,a.mobile_oper_platform,a.pay_scene order by a.gmt_occur) as user_oper_scene_dense_rank
        ,dense_rank() over(partition by a.user_id,a.mobile_oper_platform,a.device_sign order by a.gmt_occur) as user_oper_dev_dense_rank
        ,dense_rank() over(partition by a.user_id,a.device_sign,a.pay_scene order by a.gmt_occur) as user_dev_scene_dense_rank
        ,dense_rank() over(partition by a.user_id,a.device_sign,a.operation_channel order by a.gmt_occur) as user_chan_dev_dense_rank
        ,dense_rank() over(partition by a.user_id,a.pay_scene,a.operation_channel order by a.gmt_occur) as user_scene_oper_dense_rank

from atec_1000w_ins_data as a
left join
(
    select user_id,gmt_occur,count(1) as user_count_in_hour
     from atec_1000w_ins_data
     group by user_id,gmt_occur
    ) as b
on a.user_id = b.user_id and a.gmt_occur = b.gmt_occur
left join
(
    select user_id,gmt_occur,client_ip,count(1) as ip_count_in_hour
     from atec_1000w_ins_data
     group by user_id,gmt_occur,client_ip   
    ) as c
on a.user_id = c.user_id and a.gmt_occur = c.gmt_occur and a.client_ip = c.client_ip
left join
(
    select user_id,gmt_occur,device_sign,count(1) as device_count_in_hour
     from atec_1000w_ins_data
     group by user_id,gmt_occur,device_sign   
    ) as d
on a.user_id = d.user_id and a.gmt_occur = d.gmt_occur and a.device_sign = d.device_sign
left join
(
    select user_id,gmt_occur,sum(amt) as amt_sum_in_hour
     from atec_1000w_ins_data
     group by user_id,gmt_occur   
    ) as e
on a.user_id = e.user_id and a.gmt_occur = e.gmt_occur 
left join
(
    select user_id,gmt_occur,pay_scene,count(1) as pay_sence_count_in_hour
     from atec_1000w_ins_data
     group by user_id,gmt_occur,pay_scene   
    ) as f
on a.user_id = f.user_id and a.gmt_occur = f.gmt_occur and a.pay_scene = f.pay_scene
left join
(
    select gmt_occur,opposing_id,count(1) as oppo_count_in_hour
     from atec_1000w_ins_data
     group by gmt_occur,opposing_id    
    ) as h
on a.gmt_occur = h.gmt_occur and a.opposing_id = h.opposing_id
left join
(
    select gmt_occur,user_id,operation_channel,count(1) as operation_channel_count_in_hour
     from atec_1000w_ins_data
     group by gmt_occur,user_id,operation_channel    
    ) as j
on a.user_id = j.user_id and a.gmt_occur = j.gmt_occur and a.operation_channel = j.operation_channel
left join
(
    select gmt_occur,user_id,network,count(1) as network_count_in_hour
     from atec_1000w_ins_data
     group by gmt_occur,user_id,network    
    ) as l
on a.user_id = l.user_id and a.gmt_occur = l.gmt_occur and a.network = l.network
left join
(
    select gmt_occur,user_id,mobile_oper_platform,count(1) as mobile_oper_platform_count_in_hour
     from atec_1000w_ins_data
     group by gmt_occur,user_id,mobile_oper_platform    
    ) as m
on a.user_id = m.user_id and a.gmt_occur = m.gmt_occur and a.mobile_oper_platform = m.mobile_oper_platform
left join
(
    select gmt_occur,user_id,cert_prov,cert_city,count(1) as cert_prov_city_user_count_in_hour
     from atec_1000w_ins_data
     group by gmt_occur,user_id,cert_prov,cert_city    
    ) as q
on a.user_id = q.user_id and a.gmt_occur = q.gmt_occur and a.cert_prov = q.cert_prov and a.cert_city = q.cert_city
left join
(
    select gmt_occur,user_id,card_bin_prov,card_bin_city,count(1) as card_bin_prov_city_user_count_in_hour
     from atec_1000w_ins_data
     group by gmt_occur,user_id,card_bin_prov,card_bin_city   
    ) as r
on a.user_id = r.user_id and a.gmt_occur = r.gmt_occur and a.card_bin_prov = r.card_bin_prov and a.card_bin_city = r.card_bin_city
left join
(
    select gmt_occur,user_id,card_mobile_prov,card_mobile_city,count(1) as card_mobile_prov_city_user_count_in_hour
     from atec_1000w_ins_data
     group by gmt_occur,user_id,card_mobile_prov,card_mobile_city    
    ) as s
on a.user_id = s.user_id and a.gmt_occur = s.gmt_occur and a.card_mobile_prov = s.card_mobile_prov and a.card_mobile_city = s.card_mobile_city
left join
(
    select gmt_occur,user_id,card_cert_prov,card_cert_city,count(1) as card_cert_prov_city_user_count_in_hour
     from atec_1000w_ins_data
     group by gmt_occur,user_id,card_cert_prov,card_cert_city    
    ) as t
on a.user_id = t.user_id and a.gmt_occur = t.gmt_occur and a.card_cert_prov = t.card_cert_prov and a.card_cert_city = t.card_cert_city
left join
(
    select gmt_occur,user_id,mobile_oper_platform,count(1) as mobile_oper_platform_user_count_in_hour
     from atec_1000w_ins_data
     group by gmt_occur,user_id,mobile_oper_platform    
    ) as u
on a.user_id = u.user_id and a.gmt_occur = u.gmt_occur and a.mobile_oper_platform = u.mobile_oper_platform
left join
(
    select gmt_occur,user_id,opposing_id,count(1) as oppo_user_count_in_hour
     from atec_1000w_ins_data
     group by gmt_occur,user_id,opposing_id    
    ) as v
on a.user_id = v.user_id and a.gmt_occur = v.gmt_occur and a.opposing_id = v.opposing_id
left join
(
    select gmt_occur,opposing_id,card_cert_no,count(1) as oppo_card_count_in_hour
     from atec_1000w_ins_data
     group by gmt_occur,card_cert_no,opposing_id    
    ) as w
on a.card_cert_no = w.card_cert_no and a.gmt_occur = w.gmt_occur and a.opposing_id = w.opposing_id
left join
(
    select gmt_occur,opposing_id,user_id,device_sign,count(1) as user_oppo_device_count_in_hour
     from atec_1000w_ins_data
     group by gmt_occur,user_id,opposing_id,device_sign    
    ) as x
on a.user_id = x.user_id and a.gmt_occur = x.gmt_occur and a.opposing_id = x.opposing_id and a.device_sign = x.device_sign
left join
(
    select gmt_occur,income_card_no,user_id,count(1) as user_income_card_no_count_in_hour
     from atec_1000w_ins_data
     group by gmt_occur,user_id,income_card_no    
    ) as y
on a.user_id = y.user_id and a.gmt_occur = y.gmt_occur and a.income_card_no = y.income_card_no
left join
(
    select gmt_occur,opposing_id,income_card_no,count(1) as oppo_income_card_no_count_in_hour
     from atec_1000w_ins_data
     group by gmt_occur,opposing_id,income_card_no   
    ) as z
on a.opposing_id = z.opposing_id and a.gmt_occur = z.gmt_occur and a.income_card_no = z.income_card_no
left join
(
    select gmt_occur,client_ip,ip_prov,ip_city,count(1) as network_ip_count
     from atec_1000w_ins_data
     group by gmt_occur,client_ip,ip_prov,ip_city    
    ) as ta
on a.client_ip = ta.client_ip and a.gmt_occur = ta.gmt_occur and a.ip_prov = ta.ip_prov and a.ip_city = ta.ip_city
left join
(
    select gmt_occur,operation_channel,pay_scene,mobile_oper_platform,count(1) as oper_scene_platform_count
     from atec_1000w_ins_data
     group by gmt_occur,operation_channel,pay_scene,mobile_oper_platform
    ) as tb
on a.operation_channel = tb.operation_channel and a.gmt_occur = tb.gmt_occur and a.pay_scene = tb.pay_scene and a.mobile_oper_platform = tb.mobile_oper_platform
left join
(
    select gmt_occur,operation_channel,pay_scene,network,count(1) as oper_scene_network_count
     from atec_1000w_ins_data
     group by gmt_occur,operation_channel,pay_scene,network   
    ) as tc
on a.operation_channel = tc.operation_channel and a.gmt_occur = tc.gmt_occur and a.pay_scene = tc.pay_scene and a.network = tc.network
left join
(
    select gmt_occur,user_id,device_sign, pay_scene,count(1) as user_dev_scene_count_in_hour
     from atec_1000w_ins_data
     group by gmt_occur,user_id,device_sign, pay_scene    
    ) as c1
on a.device_sign = c1.device_sign and a.gmt_occur = c1.gmt_occur and a.user_id = c1.user_id and a.pay_scene = c1.pay_scene 
left join
(
    select gmt_occur,user_id,device_sign, operation_channel,count(1) as user_dev_oper_count_in_hour
     from atec_1000w_ins_data
     group by gmt_occur,user_id,device_sign, operation_channel    
    ) as c2
on a.device_sign = c2.device_sign and a.gmt_occur = c2.gmt_occur and a.user_id = c2.user_id and a.operation_channel = c2.operation_channel
left join
(
    select gmt_occur,user_id,device_sign, client_ip,count(1) as user_dev_ip_count_in_hour
     from atec_1000w_ins_data
     group by gmt_occur,user_id,device_sign, client_ip    
    ) as c3
on a.device_sign = c3.device_sign and a.gmt_occur = c3.gmt_occur and a.user_id = c3.user_id and a.client_ip = c3.client_ip
left join
(
    select gmt_occur,user_id,device_sign, network,count(1) as user_dev_net_count_in_hour
     from atec_1000w_ins_data
     group by gmt_occur,user_id,device_sign, network    
    ) as c4
on a.device_sign = c4.device_sign and a.gmt_occur = c4.gmt_occur and a.user_id = c4.user_id and a.network = c4.network
left join
(
    select gmt_occur,user_id,device_sign, mobile_oper_platform,count(1) as user_dev_plat_count_in_hour
     from atec_1000w_ins_data
     group by gmt_occur,user_id,device_sign, mobile_oper_platform    
    ) as c5
on a.device_sign = c5.device_sign and a.gmt_occur = c5.gmt_occur and a.user_id = c5.user_id and a.mobile_oper_platform = c5.mobile_oper_platform 
left join
(
    select gmt_occur,user_id,client_ip, mobile_oper_platform,count(1) as user_ip_plat_count_in_hour
     from atec_1000w_ins_data
     group by gmt_occur,user_id,client_ip, mobile_oper_platform    
    ) as c6
on a.client_ip = c6.client_ip and a.gmt_occur = c6.gmt_occur and a.user_id = c6.user_id and a.mobile_oper_platform = c6.mobile_oper_platform 
left join
(
    select gmt_occur,user_id,client_ip, pay_scene,count(1) as user_ip_scene_count_in_hour
     from atec_1000w_ins_data
     group by gmt_occur,user_id,client_ip, pay_scene    
    ) as c7
on a.client_ip = c7.client_ip and a.gmt_occur = c7.gmt_occur and a.user_id = c7.user_id and a.pay_scene = c7.pay_scene 
left join
(
    select gmt_occur,user_id,client_ip, operation_channel,count(1) as user_ip_chan_count_in_hour
     from atec_1000w_ins_data
     group by gmt_occur,user_id,client_ip, operation_channel    
    ) as c8
on a.client_ip = c8.client_ip and a.gmt_occur = c8.gmt_occur and a.user_id = c8.user_id and a.operation_channel = c8.operation_channel 
left join
(
    select gmt_occur,user_id,client_ip, network,count(1) as user_ip_net_count_in_hour
     from atec_1000w_ins_data
     group by gmt_occur,user_id,client_ip, network    
    ) as c9
on a.client_ip = c9.client_ip and a.gmt_occur = c9.gmt_occur and a.user_id = c9.user_id and a.network = c9.network 
left join
(
    select gmt_occur,user_id,operation_channel, network,count(1) as user_chan_net_count_in_hour
     from atec_1000w_ins_data
     group by gmt_occur,user_id,operation_channel, network    
    ) as c10
on a.operation_channel = c10.operation_channel and a.gmt_occur = c10.gmt_occur and a.user_id = c10.user_id and a.network = c10.network 
left join
(
    select gmt_occur,user_id,mobile_oper_platform, network,count(1) as user_plat_net_count_in_hour
     from atec_1000w_ins_data
     group by gmt_occur,user_id,mobile_oper_platform, network   
    ) as c11
on a.mobile_oper_platform = c11.mobile_oper_platform and a.gmt_occur = c11.gmt_occur and a.user_id = c11.user_id and a.network = c11.network 
left join
(
    select gmt_occur,user_id,pay_scene, network,count(1) as user_scene_net_count_in_hour
     from atec_1000w_ins_data
     group by gmt_occur,user_id,pay_scene, network    
    ) as c12
on a.pay_scene = c12.pay_scene and a.gmt_occur = c12.gmt_occur and a.user_id = c12.user_id and a.network = c12.network 
left join
(
    select gmt_occur,user_id,pay_scene, operation_channel,count(1) as user_scene_chan_count_in_hour
     from atec_1000w_ins_data
     group by gmt_occur,user_id,pay_scene, operation_channel    
    ) as c13
on a.pay_scene = c13.pay_scene and a.gmt_occur = c13.gmt_occur and a.user_id = c13.user_id and a.operation_channel = c13.operation_channel 
left join
(
    select gmt_occur,user_id,pay_scene, mobile_oper_platform,count(1) as user_plat_scene_count_in_hour
     from atec_1000w_ins_data
     group by gmt_occur,user_id,pay_scene, mobile_oper_platform    
    ) as c14
on a.pay_scene = c14.pay_scene and a.gmt_occur = c14.gmt_occur and a.user_id = c14.user_id and a.mobile_oper_platform = c14.mobile_oper_platform 
left join
(
    select gmt_occur,user_id,operation_channel, mobile_oper_platform,count(1) as user_plat_chan_count_in_hour
     from atec_1000w_ins_data
     group by gmt_occur,user_id,operation_channel, mobile_oper_platform    
    ) as c15
on a.operation_channel = c15.operation_channel and a.gmt_occur = c15.gmt_occur and a.user_id = c15.user_id and a.mobile_oper_platform = c15.mobile_oper_platform 


