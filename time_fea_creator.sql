DROP TABLE if EXISTS train_time_fea;
CREATE table train_time_fea as SELECT a.event_id,a.gmt_occur,a.user_id
,device_count_7days
,scene_count_7days
,network_count_7days
,mobile_oper_platform_count_7days
,client_ip_count_7days
,operation_channel_count_7days
,device_count_30days
,scene_count_30days
,network_count_30days
,mobile_oper_platform_count_30days
,client_ip_count_30days
,operation_channel_count_30days
,device_count_15days
,scene_count_15days
,network_count_15days
,mobile_oper_platform_count_15days
,client_ip_count_15days
,operation_channel_count_15days
from atec_1000w_ins_data as a 
left join
(
    select t1.user_id
    ,t1.device_sign
    ,t1.gmt_occur
    ,sum(case when  datediff(concat(t1.gmt_occur,':00:00'),concat(t2.gmt_occur,':00:00'),'dd') <7 then cnt else 0 end) as device_count_7days
    from 
    (
        select user_id,device_sign,gmt_occur
        from atec_1000w_ins_data
        group by user_id,device_sign,gmt_occur
        ) as t1
    left join
    (
        select user_id,device_sign,gmt_occur,count(1) as cnt 
    	from atec_1000w_ins_data
    	group by user_id,device_sign,gmt_occur
    ) as t2
    on t1.user_id = t2.user_id  and t1.device_sign = t2.device_sign
    where t1.gmt_occur>=t2.gmt_occur
    GROUP  by t1.user_id ,t1.device_sign,t1.gmt_occur
     
) as dt
on a.user_id = dt.user_id and a.gmt_occur = dt.gmt_occur and a.device_sign = dt.device_sign
left join
(
    select t1.user_id
    ,t1.pay_scene
    ,t1.gmt_occur
    ,sum(case when  datediff(concat(t1.gmt_occur,':00:00'),concat(t2.gmt_occur,':00:00'),'dd') <7 then cnt else 0 end) as scene_count_7days
    from 
    (
        select user_id,pay_scene,gmt_occur
        from atec_1000w_ins_data
        group by user_id,pay_scene,gmt_occur
        ) as t1
    left join
    (
        select user_id,pay_scene,gmt_occur,count(1) as cnt 
    	from atec_1000w_ins_data
    	group by user_id,pay_scene,gmt_occur
    ) as t2
    on t1.user_id = t2.user_id  and t1.pay_scene = t2.pay_scene
    where t1.gmt_occur>=t2.gmt_occur
    GROUP  by t1.user_id ,t1.pay_scene,t1.gmt_occur
     
) as dt1
on a.user_id = dt1.user_id and a.gmt_occur = dt1.gmt_occur and a.pay_scene = dt1.pay_scene
left join
(
    select t1.user_id
    ,t1.client_ip
    ,t1.gmt_occur
    ,sum(case when  datediff(concat(t1.gmt_occur,':00:00'),concat(t2.gmt_occur,':00:00'),'dd') <7 then cnt else 0 end) as client_ip_count_7days
    from 
    (
        select user_id,client_ip,gmt_occur
        from atec_1000w_ins_data
        group by user_id,client_ip,gmt_occur
        ) as t1
    left join
    (
        select user_id,client_ip,gmt_occur,count(1) as cnt 
    	from atec_1000w_ins_data
    	group by user_id,client_ip,gmt_occur
    ) as t2
    on t1.user_id = t2.user_id  and t1.client_ip = t2.client_ip
    where t1.gmt_occur>=t2.gmt_occur
    GROUP  by t1.user_id ,t1.client_ip,t1.gmt_occur
     
) as dt2
on a.user_id = dt2.user_id and a.gmt_occur = dt2.gmt_occur and a.client_ip = dt2.client_ip
left join
(
    select t1.user_id
    ,t1.operation_channel
    ,t1.gmt_occur
    ,sum(case when  datediff(concat(t1.gmt_occur,':00:00'),concat(t2.gmt_occur,':00:00'),'dd') <7 then cnt else 0 end) as operation_channel_count_7days
    from 
    (
        select user_id,operation_channel,gmt_occur
        from atec_1000w_ins_data
        group by user_id,operation_channel,gmt_occur
        ) as t1
    left join
    (
        select user_id,operation_channel,gmt_occur,count(1) as cnt 
    	from atec_1000w_ins_data
    	group by user_id,operation_channel,gmt_occur
    ) as t2
    on t1.user_id = t2.user_id  and t1.operation_channel = t2.operation_channel
    where t1.gmt_occur>=t2.gmt_occur
    GROUP  by t1.user_id ,t1.operation_channel,t1.gmt_occur
     
) as dt3
on a.user_id = dt3.user_id and a.gmt_occur = dt3.gmt_occur and a.operation_channel = dt3.operation_channel
left join
(
    select t1.user_id
    ,t1.mobile_oper_platform
    ,t1.gmt_occur
    ,sum(case when  datediff(concat(t1.gmt_occur,':00:00'),concat(t2.gmt_occur,':00:00'),'dd') <7 then cnt else 0 end) as mobile_oper_platform_count_7days
    from 
    (
        select user_id,mobile_oper_platform,gmt_occur
        from atec_1000w_ins_data
        group by user_id,mobile_oper_platform,gmt_occur
        ) as t1
    left join
    (
        select user_id,mobile_oper_platform,gmt_occur,count(1) as cnt 
    	from atec_1000w_ins_data
    	group by user_id,mobile_oper_platform,gmt_occur
    ) as t2
    on t1.user_id = t2.user_id  and t1.mobile_oper_platform = t2.mobile_oper_platform
    where t1.gmt_occur>=t2.gmt_occur
    GROUP  by t1.user_id ,t1.mobile_oper_platform,t1.gmt_occur
     
) as dt4
on a.user_id = dt4.user_id and a.gmt_occur = dt4.gmt_occur and a.mobile_oper_platform = dt4.mobile_oper_platform
left join
(
    select t1.user_id
    ,t1.network
    ,t1.gmt_occur
    ,sum(case when  datediff(concat(t1.gmt_occur,':00:00'),concat(t2.gmt_occur,':00:00'),'dd') <7 then cnt else 0 end) as network_count_7days
    from 
    (
        select user_id,network,gmt_occur
        from atec_1000w_ins_data
        group by user_id,network,gmt_occur
        ) as t1
    left join
    (
        select user_id,network,gmt_occur,count(1) as cnt 
    	from atec_1000w_ins_data
    	group by user_id,network,gmt_occur
    ) as t2
    on t1.user_id = t2.user_id  and t1.network = t2.network
    where t1.gmt_occur>=t2.gmt_occur
    GROUP  by t1.user_id ,t1.network,t1.gmt_occur
     
) as dt5
on a.user_id = dt5.user_id and a.gmt_occur = dt5.gmt_occur and a.network = dt5.network
left join
(
    select t1.user_id
    ,t1.device_sign
    ,t1.gmt_occur
    ,sum(case when  datediff(concat(t1.gmt_occur,':00:00'),concat(t2.gmt_occur,':00:00'),'dd') <30 then cnt else 0 end) as device_count_30days
    from 
    (
        select user_id,device_sign,gmt_occur
        from atec_1000w_ins_data
        group by user_id,device_sign,gmt_occur
        ) as t1
    left join
    (
        select user_id,device_sign,gmt_occur,count(1) as cnt 
    	from atec_1000w_ins_data
    	group by user_id,device_sign,gmt_occur
    ) as t2
    on t1.user_id = t2.user_id  and t1.device_sign = t2.device_sign
    where t1.gmt_occur>=t2.gmt_occur
    GROUP  by t1.user_id ,t1.device_sign,t1.gmt_occur
     
) as ctt
on a.user_id = ctt.user_id and a.gmt_occur = ctt.gmt_occur and a.device_sign = ctt.device_sign
left join
(
    select t1.user_id
    ,t1.pay_scene
    ,t1.gmt_occur
    ,sum(case when  datediff(concat(t1.gmt_occur,':00:00'),concat(t2.gmt_occur,':00:00'),'dd') <30 then cnt else 0 end) as scene_count_30days
    from 
    (
        select user_id,pay_scene,gmt_occur
        from atec_1000w_ins_data
        group by user_id,pay_scene,gmt_occur
        ) as t1
    left join
    (
        select user_id,pay_scene,gmt_occur,count(1) as cnt 
    	from atec_1000w_ins_data
    	group by user_id,pay_scene,gmt_occur
    ) as t2
    on t1.user_id = t2.user_id  and t1.pay_scene = t2.pay_scene
    where t1.gmt_occur>=t2.gmt_occur
    GROUP  by t1.user_id ,t1.pay_scene,t1.gmt_occur
     
) as ctt1
on a.user_id = ctt1.user_id and a.gmt_occur = ctt1.gmt_occur and a.pay_scene = ctt1.pay_scene
left join
(
    select t1.user_id
    ,t1.client_ip
    ,t1.gmt_occur
    ,sum(case when  datediff(concat(t1.gmt_occur,':00:00'),concat(t2.gmt_occur,':00:00'),'dd') <30 then cnt else 0 end) as client_ip_count_30days
    from 
    (
        select user_id,client_ip,gmt_occur
        from atec_1000w_ins_data
        group by user_id,client_ip,gmt_occur
        ) as t1
    left join
    (
        select user_id,client_ip,gmt_occur,count(1) as cnt 
    	from atec_1000w_ins_data
    	group by user_id,client_ip,gmt_occur
    ) as t2
    on t1.user_id = t2.user_id  and t1.client_ip = t2.client_ip
    where t1.gmt_occur>=t2.gmt_occur
    GROUP  by t1.user_id ,t1.client_ip,t1.gmt_occur
     
) as ctt2
on a.user_id = ctt2.user_id and a.gmt_occur = ctt2.gmt_occur and a.client_ip = ctt2.client_ip
left join
(
    select t1.user_id
    ,t1.operation_channel
    ,t1.gmt_occur
    ,sum(case when  datediff(concat(t1.gmt_occur,':00:00'),concat(t2.gmt_occur,':00:00'),'dd') <30 then cnt else 0 end) as operation_channel_count_30days
    from 
    (
        select user_id,operation_channel,gmt_occur
        from atec_1000w_ins_data
        group by user_id,operation_channel,gmt_occur
        ) as t1
    left join
    (
        select user_id,operation_channel,gmt_occur,count(1) as cnt 
    	from atec_1000w_ins_data
    	group by user_id,operation_channel,gmt_occur
    ) as t2
    on t1.user_id = t2.user_id  and t1.operation_channel = t2.operation_channel
    where t1.gmt_occur>=t2.gmt_occur
    GROUP  by t1.user_id ,t1.operation_channel,t1.gmt_occur
     
) as ctt3
on a.user_id = ctt3.user_id and a.gmt_occur = ctt3.gmt_occur and a.operation_channel = ctt3.operation_channel
left join
(
    select t1.user_id
    ,t1.mobile_oper_platform
    ,t1.gmt_occur
    ,sum(case when  datediff(concat(t1.gmt_occur,':00:00'),concat(t2.gmt_occur,':00:00'),'dd') <30 then cnt else 0 end) as mobile_oper_platform_count_30days
    from 
    (
        select user_id,mobile_oper_platform,gmt_occur
        from atec_1000w_ins_data
        group by user_id,mobile_oper_platform,gmt_occur
        ) as t1
    left join
    (
        select user_id,mobile_oper_platform,gmt_occur,count(1) as cnt 
    	from atec_1000w_ins_data
    	group by user_id,mobile_oper_platform,gmt_occur
    ) as t2
    on t1.user_id = t2.user_id  and t1.mobile_oper_platform = t2.mobile_oper_platform
    where t1.gmt_occur>=t2.gmt_occur
    GROUP  by t1.user_id ,t1.mobile_oper_platform,t1.gmt_occur
     
) as ctt4
on a.user_id = ctt4.user_id and a.gmt_occur = ctt4.gmt_occur and a.mobile_oper_platform = ctt4.mobile_oper_platform
left join
(
    select t1.user_id
    ,t1.network
    ,t1.gmt_occur
    ,sum(case when  datediff(concat(t1.gmt_occur,':00:00'),concat(t2.gmt_occur,':00:00'),'dd') <30 then cnt else 0 end) as network_count_30days
    from 
    (
        select user_id,network,gmt_occur
        from atec_1000w_ins_data
        group by user_id,network,gmt_occur
        ) as t1
    left join
    (
        select user_id,network,gmt_occur,count(1) as cnt 
    	from atec_1000w_ins_data
    	group by user_id,network,gmt_occur
    ) as t2
    on t1.user_id = t2.user_id  and t1.network = t2.network
    where t1.gmt_occur>=t2.gmt_occur
    GROUP  by t1.user_id ,t1.network,t1.gmt_occur
     
) as ctt5
on a.user_id = ctt5.user_id and a.gmt_occur = ctt5.gmt_occur and a.network = ctt5.network
left join
(
    select t1.user_id
    ,t1.device_sign
    ,t1.gmt_occur
    ,sum(case when  datediff(concat(t1.gmt_occur,':00:00'),concat(t2.gmt_occur,':00:00'),'dd') <15 then cnt else 0 end) as device_count_15days
    from 
    (
        select user_id,device_sign,gmt_occur
        from atec_1000w_ins_data
        group by user_id,device_sign,gmt_occur
        ) as t1
    left join
    (
        select user_id,device_sign,gmt_occur,count(1) as cnt 
    	from atec_1000w_ins_data
    	group by user_id,device_sign,gmt_occur
    ) as t2
    on t1.user_id = t2.user_id  and t1.device_sign = t2.device_sign
    where t1.gmt_occur>=t2.gmt_occur
    GROUP  by t1.user_id ,t1.device_sign,t1.gmt_occur
     
) as d15t
on a.user_id = d15t.user_id and a.gmt_occur = d15t.gmt_occur and a.device_sign = d15t.device_sign
left join
(
    select t1.user_id
    ,t1.pay_scene
    ,t1.gmt_occur
    ,sum(case when  datediff(concat(t1.gmt_occur,':00:00'),concat(t2.gmt_occur,':00:00'),'dd') <15 then cnt else 0 end) as scene_count_15days
    from 
    (
        select user_id,pay_scene,gmt_occur
        from atec_1000w_ins_data
        group by user_id,pay_scene,gmt_occur
        ) as t1
    left join
    (
        select user_id,pay_scene,gmt_occur,count(1) as cnt 
    	from atec_1000w_ins_data
    	group by user_id,pay_scene,gmt_occur
    ) as t2
    on t1.user_id = t2.user_id  and t1.pay_scene = t2.pay_scene
    where t1.gmt_occur>=t2.gmt_occur
    GROUP  by t1.user_id ,t1.pay_scene,t1.gmt_occur
     
) as d15t1
on a.user_id = d15t1.user_id and a.gmt_occur = d15t1.gmt_occur and a.pay_scene = d15t1.pay_scene
left join
(
    select t1.user_id
    ,t1.client_ip
    ,t1.gmt_occur
    ,sum(case when  datediff(concat(t1.gmt_occur,':00:00'),concat(t2.gmt_occur,':00:00'),'dd') <15 then cnt else 0 end) as client_ip_count_15days
    from 
    (
        select user_id,client_ip,gmt_occur
        from atec_1000w_ins_data
        group by user_id,client_ip,gmt_occur
        ) as t1
    left join
    (
        select user_id,client_ip,gmt_occur,count(1) as cnt 
    	from atec_1000w_ins_data
    	group by user_id,client_ip,gmt_occur
    ) as t2
    on t1.user_id = t2.user_id  and t1.client_ip = t2.client_ip
    where t1.gmt_occur>=t2.gmt_occur
    GROUP  by t1.user_id ,t1.client_ip,t1.gmt_occur
     
) as d15t2
on a.user_id = d15t2.user_id and a.gmt_occur = d15t2.gmt_occur and a.client_ip = d15t2.client_ip
left join
(
    select t1.user_id
    ,t1.operation_channel
    ,t1.gmt_occur
    ,sum(case when  datediff(concat(t1.gmt_occur,':00:00'),concat(t2.gmt_occur,':00:00'),'dd') <15 then cnt else 0 end) as operation_channel_count_15days
    from 
    (
        select user_id,operation_channel,gmt_occur
        from atec_1000w_ins_data
        group by user_id,operation_channel,gmt_occur
        ) as t1
    left join
    (
        select user_id,operation_channel,gmt_occur,count(1) as cnt 
    	from atec_1000w_ins_data
    	group by user_id,operation_channel,gmt_occur
    ) as t2
    on t1.user_id = t2.user_id  and t1.operation_channel = t2.operation_channel
    where t1.gmt_occur>=t2.gmt_occur
    GROUP  by t1.user_id ,t1.operation_channel,t1.gmt_occur
     
) as d15t3
on a.user_id = d15t3.user_id and a.gmt_occur = d15t3.gmt_occur and a.operation_channel = d15t3.operation_channel
left join
(
    select t1.user_id
    ,t1.mobile_oper_platform
    ,t1.gmt_occur
    ,sum(case when  datediff(concat(t1.gmt_occur,':00:00'),concat(t2.gmt_occur,':00:00'),'dd') <15 then cnt else 0 end) as mobile_oper_platform_count_15days
    from 
    (
        select user_id,mobile_oper_platform,gmt_occur
        from atec_1000w_ins_data
        group by user_id,mobile_oper_platform,gmt_occur
        ) as t1
    left join
    (
        select user_id,mobile_oper_platform,gmt_occur,count(1) as cnt 
    	from atec_1000w_ins_data
    	group by user_id,mobile_oper_platform,gmt_occur
    ) as t2
    on t1.user_id = t2.user_id  and t1.mobile_oper_platform = t2.mobile_oper_platform
    where t1.gmt_occur>=t2.gmt_occur
    GROUP  by t1.user_id ,t1.mobile_oper_platform,t1.gmt_occur
     
) as d15t4
on a.user_id = d15t4.user_id and a.gmt_occur = d15t4.gmt_occur and a.mobile_oper_platform = d15t4.mobile_oper_platform
left join
(
    select t1.user_id
    ,t1.network
    ,t1.gmt_occur
    ,sum(case when  datediff(concat(t1.gmt_occur,':00:00'),concat(t2.gmt_occur,':00:00'),'dd') <15 then cnt else 0 end) as network_count_15days
    from 
    (
        select user_id,network,gmt_occur
        from atec_1000w_ins_data
        group by user_id,network,gmt_occur
        ) as t1
    left join
    (
        select user_id,network,gmt_occur,count(1) as cnt 
    	from atec_1000w_ins_data
    	group by user_id,network,gmt_occur
    ) as t2
    on t1.user_id = t2.user_id  and t1.network = t2.network
    where t1.gmt_occur>=t2.gmt_occur
    GROUP  by t1.user_id ,t1.network,t1.gmt_occur
     
) as d15t5
on a.user_id = d15t5.user_id and a.gmt_occur = d15t5.gmt_occur and a.network = d15t5.network
--30天内，当前用户使用当前设备的次数
