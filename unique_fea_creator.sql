DROP TABLE if EXISTS train_unique_fea;
CREATE table train_unique_fea as SELECT a.event_id,a.gmt_occur,a.user_id
,device_sign_unique
,pay_scene_unique
,operation_channel_unique
,client_ip_unique
,mobile_oper_platform_unique
,network_unique
,ip_dev_unique
,scene_dev_unique
,plat_dev_unique
,chan_dev_unique
,chan_plat_unique
,scene_chan_plat_unique
,scene_chan_unique
,scene_oper_unique
,client_ip_prov_city_unique
,ip_prov_city_unique
,cert_prov_city_unique
,card_mobile_prov_city_unique
,card_cert_prov_city_unique
from atec_1000w_ins_data as a 
left join(
    select t1.user_id ,t1.gmt_occur
    ,count(distinct t2.device_sign) as device_sign_unique
    from 
    (select user_id,gmt_occur
     from atec_1000w_ins_data
     group by user_id,gmt_occur) as t1
    left join 
    (select user_id,device_sign,gmt_occur
     from atec_1000w_ins_data
     group by user_id,device_sign,gmt_occur) as t2
    on t1.user_id = t2.user_id
    where t1.gmt_occur>=t2.gmt_occur 
    group by t1.user_id,t1.gmt_occur
) as at1
on a.user_id = at1.user_id and a.gmt_occur = at1.gmt_occur
left join(
    select t1.user_id ,t1.gmt_occur
    ,count(distinct t2.pay_scene) as pay_scene_unique
    from 
    (select user_id,gmt_occur
     from atec_1000w_ins_data
     group by user_id,gmt_occur) as t1
    left join 
    (select user_id,pay_scene,gmt_occur
     from atec_1000w_ins_data
     group by user_id,pay_scene,gmt_occur) as t2
    on t1.user_id = t2.user_id
    where t1.gmt_occur>=t2.gmt_occur 
    group by t1.user_id,t1.gmt_occur
) as at2
on a.user_id = at2.user_id and a.gmt_occur = at2.gmt_occur
left join(
    select t1.user_id ,t1.gmt_occur
    ,count(distinct t2.operation_channel) as operation_channel_unique
    from 
    (select user_id,gmt_occur
     from atec_1000w_ins_data
     group by user_id,gmt_occur) as t1
    left join 
    (select user_id,operation_channel,gmt_occur
     from atec_1000w_ins_data
     group by user_id,operation_channel,gmt_occur) as t2
    on t1.user_id = t2.user_id
    where t1.gmt_occur>=t2.gmt_occur 
    group by t1.user_id,t1.gmt_occur
) as at3
on a.user_id = at3.user_id and a.gmt_occur = at3.gmt_occur
left join(
    select t1.user_id ,t1.gmt_occur
    ,count(distinct t2.client_ip) as client_ip_unique
    from 
    (select user_id,gmt_occur
     from atec_1000w_ins_data
     group by user_id,gmt_occur) as t1
    left join 
    (select user_id,client_ip,gmt_occur
     from atec_1000w_ins_data
     group by user_id,client_ip,gmt_occur) as t2
    on t1.user_id = t2.user_id
    where t1.gmt_occur>=t2.gmt_occur 
    group by t1.user_id,t1.gmt_occur
) as at4
on a.user_id = at4.user_id and a.gmt_occur = at4.gmt_occur
left join(
    select t1.user_id ,t1.gmt_occur
    ,count(distinct t2.mobile_oper_platform) as mobile_oper_platform_unique
    from 
    (select user_id,gmt_occur
     from atec_1000w_ins_data
     group by user_id,gmt_occur) as t1
    left join 
    (select user_id,mobile_oper_platform,gmt_occur
     from atec_1000w_ins_data
     group by user_id,mobile_oper_platform,gmt_occur) as t2
    on t1.user_id = t2.user_id
    where t1.gmt_occur>=t2.gmt_occur 
    group by t1.user_id,t1.gmt_occur
) as at5
on a.user_id = at5.user_id and a.gmt_occur = at5.gmt_occur
left join(
    select t1.user_id ,t1.gmt_occur
    ,count(distinct t2.network) as network_unique
    from 
    (select user_id,gmt_occur
     from atec_1000w_ins_data
     group by user_id,gmt_occur) as t1
    left join 
    (select user_id,network,gmt_occur
     from atec_1000w_ins_data
     group by user_id,network,gmt_occur) as t2
    on t1.user_id = t2.user_id
    where t1.gmt_occur>=t2.gmt_occur 
    group by t1.user_id,t1.gmt_occur
) as at6
on a.user_id = at6.user_id and a.gmt_occur = at6.gmt_occur
left join(
    select t1.user_id ,t1.gmt_occur
    ,count(distinct t2.device_sign,t2.client_ip) as ip_dev_unique
    from 
    (select user_id,gmt_occur
     from atec_1000w_ins_data
     group by user_id,gmt_occur) as t1
    left join 
    (select user_id,device_sign,client_ip,gmt_occur
     from atec_1000w_ins_data
     group by user_id,device_sign,client_ip,gmt_occur) as t2
    on t1.user_id = t2.user_id
    where t1.gmt_occur>=t2.gmt_occur 
    group by t1.user_id,t1.gmt_occur
) as ct1
on a.user_id = ct1.user_id and a.gmt_occur = ct1.gmt_occur
left join(
    select t1.user_id ,t1.gmt_occur
    ,count(distinct t2.device_sign,t2.pay_scene) as scene_dev_unique
    from 
    (select user_id,gmt_occur
     from atec_1000w_ins_data
     group by user_id,gmt_occur) as t1
    left join 
    (select user_id,device_sign,pay_scene,gmt_occur
     from atec_1000w_ins_data
     group by user_id,device_sign,pay_scene,gmt_occur) as t2
    on t1.user_id = t2.user_id
    where t1.gmt_occur>=t2.gmt_occur 
    group by t1.user_id,t1.gmt_occur
) as ct2
on a.user_id = ct2.user_id and a.gmt_occur = ct2.gmt_occur
left join(
    select t1.user_id ,t1.gmt_occur
    ,count(distinct t2.device_sign,t2.mobile_oper_platform) as plat_dev_unique
    from 
    (select user_id,gmt_occur
     from atec_1000w_ins_data
     group by user_id,gmt_occur) as t1
    left join 
    (select user_id,device_sign,mobile_oper_platform,gmt_occur
     from atec_1000w_ins_data
     group by user_id,device_sign,mobile_oper_platform,gmt_occur) as t2
    on t1.user_id = t2.user_id
    where t1.gmt_occur>=t2.gmt_occur 
    group by t1.user_id,t1.gmt_occur
) as ct3
on a.user_id = ct3.user_id and a.gmt_occur = ct3.gmt_occur
left join(
    select t1.user_id ,t1.gmt_occur
    ,count(distinct t2.device_sign,t2.operation_channel) as chan_dev_unique
    from 
    (select user_id,gmt_occur
     from atec_1000w_ins_data
     group by user_id,gmt_occur) as t1
    left join 
    (select user_id,device_sign,operation_channel,gmt_occur
     from atec_1000w_ins_data
     group by user_id,device_sign,operation_channel,gmt_occur) as t2
    on t1.user_id = t2.user_id
    where t1.gmt_occur>=t2.gmt_occur 
    group by t1.user_id,t1.gmt_occur
) as ct4
on a.user_id = ct4.user_id and a.gmt_occur = ct4.gmt_occur
left join(
    select t1.user_id ,t1.gmt_occur
    ,count(distinct t2.mobile_oper_platform,t2.operation_channel) as chan_plat_unique
    from 
    (select user_id,gmt_occur
     from atec_1000w_ins_data
     group by user_id,gmt_occur) as t1
    left join 
    (select user_id,mobile_oper_platform,operation_channel,gmt_occur
     from atec_1000w_ins_data
     group by user_id,mobile_oper_platform,operation_channel,gmt_occur) as t2
    on t1.user_id = t2.user_id
    where t1.gmt_occur>=t2.gmt_occur 
    group by t1.user_id,t1.gmt_occur
) as ct5
on a.user_id = ct5.user_id and a.gmt_occur = ct5.gmt_occur
left join(
    select t1.user_id ,t1.gmt_occur
    ,count(distinct t2.mobile_oper_platform,t2.operation_channel,t2.pay_scene) as scene_chan_plat_unique
    from 
    (select user_id,gmt_occur
     from atec_1000w_ins_data
     group by user_id,gmt_occur) as t1
    left join 
    (select user_id,mobile_oper_platform,operation_channel,gmt_occur,pay_scene
     from atec_1000w_ins_data
     group by user_id,mobile_oper_platform,operation_channel,gmt_occur,pay_scene) as t2
    on t1.user_id = t2.user_id
    where t1.gmt_occur>=t2.gmt_occur 
    group by t1.user_id,t1.gmt_occur
) as ct6
on a.user_id = ct6.user_id and a.gmt_occur = ct6.gmt_occur
left join(
    select t1.user_id ,t1.gmt_occur
    ,count(distinct t2.operation_channel,t2.pay_scene) as scene_chan_unique
    from 
    (select user_id,gmt_occur
     from atec_1000w_ins_data
     group by user_id,gmt_occur) as t1
    left join 
    (select user_id,operation_channel,gmt_occur,pay_scene
     from atec_1000w_ins_data
     group by user_id,operation_channel,gmt_occur,pay_scene) as t2
    on t1.user_id = t2.user_id
    where t1.gmt_occur>=t2.gmt_occur 
    group by t1.user_id,t1.gmt_occur
) as ct7
on a.user_id = ct7.user_id and a.gmt_occur = ct7.gmt_occur
left join(
    select t1.user_id ,t1.gmt_occur
    ,count(distinct t2.mobile_oper_platform,t2.pay_scene) as scene_oper_unique
    from 
    (select user_id,gmt_occur
     from atec_1000w_ins_data
     group by user_id,gmt_occur) as t1
    left join 
    (select user_id,mobile_oper_platform,gmt_occur,pay_scene
     from atec_1000w_ins_data
     group by user_id,mobile_oper_platform,gmt_occur,pay_scene) as t2
    on t1.user_id = t2.user_id
    where t1.gmt_occur>=t2.gmt_occur 
    group by t1.user_id,t1.gmt_occur
) as ct8
on a.user_id = ct8.user_id and a.gmt_occur = ct8.gmt_occur

left join(
    select t1.user_id ,t1.gmt_occur
    ,count(distinct t2.ip_prov,t2.ip_city) as ip_prov_city_unique
    from 
    (select user_id,gmt_occur
     from atec_1000w_ins_data
     group by user_id,gmt_occur) as t1
    left join 
    (select user_id,ip_prov,gmt_occur,ip_city
     from atec_1000w_ins_data
     group by user_id,ip_prov,gmt_occur,ip_city) as t2
    on t1.user_id = t2.user_id
    where t1.gmt_occur>=t2.gmt_occur 
    group by t1.user_id,t1.gmt_occur
) as ipt1
on a.user_id = ipt1.user_id and a.gmt_occur = ipt1.gmt_occur
left join(
    select t1.user_id ,t1.gmt_occur
    ,count(distinct t2.cert_prov,t2.cert_city) as cert_prov_city_unique
    from 
    (select user_id,gmt_occur
     from atec_1000w_ins_data
     group by user_id,gmt_occur) as t1
    left join 
    (select user_id,cert_prov,gmt_occur,cert_city
     from atec_1000w_ins_data
     group by user_id,cert_prov,gmt_occur,cert_city) as t2
    on t1.user_id = t2.user_id
    where t1.gmt_occur>=t2.gmt_occur 
    group by t1.user_id,t1.gmt_occur
) as ipt2
on a.user_id = ipt2.user_id and a.gmt_occur = ipt2.gmt_occur
left join(
    select t1.user_id ,t1.gmt_occur
    ,count(distinct t2.card_cert_prov,t2.card_cert_city) as card_cert_prov_city_unique
    from 
    (select user_id,gmt_occur
     from atec_1000w_ins_data
     group by user_id,gmt_occur) as t1
    left join 
    (select user_id,card_cert_prov,gmt_occur,card_cert_city
     from atec_1000w_ins_data
     group by user_id,card_cert_prov,gmt_occur,card_cert_city) as t2
    on t1.user_id = t2.user_id
    where t1.gmt_occur>=t2.gmt_occur 
    group by t1.user_id,t1.gmt_occur
) as ipt3
on a.user_id = ipt3.user_id and a.gmt_occur = ipt3.gmt_occur
left join(
    select t1.user_id ,t1.gmt_occur
    ,count(distinct t2.card_mobile_prov,t2.card_mobile_city) as card_mobile_prov_city_unique
    from 
    (select user_id,gmt_occur
     from atec_1000w_ins_data
     group by user_id,gmt_occur) as t1
    left join 
    (select user_id,card_mobile_prov,gmt_occur,card_mobile_city
     from atec_1000w_ins_data
     group by user_id,card_mobile_prov,gmt_occur,card_mobile_city) as t2
    on t1.user_id = t2.user_id
    where t1.gmt_occur>=t2.gmt_occur 
    group by t1.user_id,t1.gmt_occur
) as ipt4
on a.user_id = ipt4.user_id and a.gmt_occur = ipt4.gmt_occur
left join(
    select t1.user_id ,t1.gmt_occur
    ,count(distinct t2.ip_prov,t2.ip_city,t2.client_ip) as client_ip_prov_city_unique
    from 
    (select user_id,gmt_occur
     from atec_1000w_ins_data
     group by user_id,gmt_occur) as t1
    left join 
    (select user_id,ip_prov,gmt_occur,ip_city,client_ip
     from atec_1000w_ins_data
     group by user_id,ip_prov,gmt_occur,ip_city,client_ip) as t2
    on t1.user_id = t2.user_id
    where t1.gmt_occur>=t2.gmt_occur 
    group by t1.user_id,t1.gmt_occur
) as ipt5
on a.user_id = ipt5.user_id and a.gmt_occur = ipt5.gmt_occur

