

--退订用户数	统计周期内，成功退续订业务的用户数。维度：部门、业务、终端、省份、地市、渠道、付费渠道、货币类型、是否虚拟局数据、订购周期、订购周期单位、是否自动续订	

--count(distinct order_user_id) where src_file_day between xxx and order_status in (12,14,15,16)	

--rptdata.fact_order_item_detail

--新增订购用户数	统计周期内，成功订购业务的用户数。维度：部门、业务、终端、省份、地市、渠道、付费渠道、货币类型、是否虚拟局数据、订购周期、订购周期单位、是否自动续订

--count(distinct order_user_id) where src_file_day between xxx and order_status in (5,9)	

--rptdata.fact_order_item_detail

--5月31日之前新增/退订的用户订单
--cdmp_dw.td_aaa_order_log_d order_type = 1新增 order_type = 2退订



with new_add_order_user as( --当月新增
select order_crt_time    
      ,sub_business_id
      ,order_msisdn_region_id
      ,payment_msisdn_region_id
      ,channel_id
      ,order_user_id
  from (
        select a.order_crt_time    
              ,a.sub_business_id
              ,a.order_msisdn_region_id
              ,a.payment_msisdn_region_id
              ,a.channel_id
              ,a.order_user_id
          from rptdata.fact_order_item_detail a 
         where '${EXTRACT_MONTH}' >=  '201706'
           and a.order_status in (5,9)   --新增 flag
           and a.src_file_day between '${EXTRACT_MONTH}01' and '${EXTRACT_MONTH}31'
           
         union all
         
        select a.opr_time as order_crt_time
              ,a.sub_busi_id as sub_business_id
              ,a.region_id as order_msisdn_region_id
              ,a.region_id as payment_msisdn_region_id
              ,a.chn_id_new as channel_id
              ,b.user_id as order_user_id
          from cdmp_dw.td_aaa_order_log_d a 
          join rptdata.dim_userid_usernum b 
            on a.serv_number = b.user_num
         where '${EXTRACT_MONTH}' <=  '201705' 
           and a.order_type = 1          --新增 flag
           and a.src_source_day between '${EXTRACT_MONTH}01' and '${EXTRACT_MONTH}31'   
       ) t 
 group by order_crt_time    
         ,sub_business_id
         ,order_msisdn_region_id
         ,payment_msisdn_region_id
         ,channel_id
         ,order_user_id
),
this_mon_cancel_order_user as (   --当月退订
select order_cancel_time    
      ,sub_business_id
      ,order_msisdn_region_id
      ,payment_msisdn_region_id
      ,channel_id
      ,order_user_id
  from (
        select a.order_last_upt_time as order_cancel_time
              ,a.sub_business_id
              ,a.order_msisdn_region_id
              ,a.payment_msisdn_region_id
              ,a.channel_id
              ,a.order_user_id
          from rptdata.fact_order_item_detail a 
         where '${EXTRACT_MONTH}' >=  '201706'
           and a.order_status in (12,14,15,16) --退订 flag
           and a.src_file_day between '${EXTRACT_MONTH}01' and '${EXTRACT_MONTH}31'
           
         union all
         
        select a.opr_time as order_cancel_time
              ,a.sub_busi_id as sub_business_id
              ,a.region_id as order_msisdn_region_id
              ,a.region_id as payment_msisdn_region_id
              ,a.chn_id_new as channel_id 
              ,b.user_id as order_user_id 
          from cdmp_dw.td_aaa_order_log_d a 
          join rptdata.dim_userid_usernum b
            on a.serv_number = b.user_num
         where '${EXTRACT_MONTH}' <=  '201705'
           and a.order_type = 2                 --退订 flag
           and a.src_source_day between '${EXTRACT_MONTH}01' and '${EXTRACT_MONTH}31'   
       ) t 
 group by order_crt_time    
         ,sub_business_id
         ,order_msisdn_region_id
         ,payment_msisdn_region_id
         ,channel_id
         ,order_user_id           
)
insert overwrite table stg.fact_this_mon_retention_user    --当月留存用户记录
select a.sub_business_id
      ,a.order_msisdn_region_id
      ,a.payment_msisdn_region_id
      ,a.channel_id
      ,a.order_user_id 
  from new_add_order_user a 
  left join this_mon_cancel_order_user b 
    on (a.sub_business_id = b.sub_business_id
        and a.order_msisdn_region_id = b.order_msisdn_region_id 
        and a.payment_msisdn_region_id = b.payment_msisdn_region_id
        and a.channel_id = b.channel_id
        and a.order_user_id = b.order_user_id)
 where a.order_crt_time < b.order_cancel_time
   and b.order_user_id is null
 group by a.sub_business_id
         ,a.order_msisdn_region_id
         ,a.payment_msisdn_region_id
         ,a.channel_id
         ,a.order_user_id;

=============================================================================================================

second_mon_cancel_order_user as (   --次月退订
select sub_business_id
      ,order_msisdn_region_id
      ,payment_msisdn_region_id
      ,channel_id
      ,order_user_id
  from (
        select a.sub_business_id
              ,a.order_msisdn_region_id
              ,a.payment_msisdn_region_id
              ,a.channel_id
              ,a.order_user_id
          from rptdata.fact_order_item_detail a 
         where translate(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),1), '-', '')  >=  '20170601'
           and a.order_status in (12,14,15,16) --退订
           and a.src_file_day between translate(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),1), '-', '') 
                              and translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),1)), '-', '')
           
         union all
         
        select a.sub_busi_id as sub_business_id
              ,a.region_id as order_msisdn_region_id
              ,a.region_id as payment_msisdn_region_id
              ,a.chn_id_new as channel_id
              ,b.user_id as order_user_id
          from cdmp_dw.td_aaa_order_log_d a 
          join rptdata.dim_userid_usernum b
            on a.serv_number = b.user_num
         where translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),1)), '-', '') <=  '20170531'
           and a.order_type = 2                 --退订
           and a.src_source_day between translate(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),1), '-', '') 
                                and translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),1)), '-', '')
       ) t
 group by sub_business_id
         ,order_msisdn_region_id
         ,payment_msisdn_region_id
         ,channel_id
         ,order_user_id     
)

--translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),1)), '-', '')   --次月最后一天
--translate(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),1), '-', '')  --次月第一天

insert overwrite table stg.fact_second_mon_retention_user    --次月留存用户记录
select a.sub_business_id
      ,a.order_msisdn_region_id
      ,a.payment_msisdn_region_id
      ,a.channel_id
      ,a.order_user_id 
  from stg.fact_this_mon_retention_user a
  left join second_mon_cancel_order_user b
    on (a.sub_business_id = b.sub_business_id
        and a.order_msisdn_region_id = b.order_msisdn_region_id 
        and a.payment_msisdn_region_id = b.payment_msisdn_region_id
        and a.channel_id = b.channel_id
        and a.order_user_id = b.order_user_id)
 where b.order_user_id is null 
 group by a.sub_business_id 
         ,a.order_msisdn_region_id
         ,a.payment_msisdn_region_id
         ,a.channel_id
         ,a.order_user_id;

==================================================================================================================

after_2_mon_cancel_order_user as (   --2个月后退订
select sub_business_id
      ,order_msisdn_region_id
      ,payment_msisdn_region_id
      ,channel_id
      ,order_user_id
  from (
        select a.sub_business_id
              ,a.order_msisdn_region_id
              ,a.payment_msisdn_region_id
              ,a.channel_id
              ,a.order_user_id
          from rptdata.fact_order_item_detail a 
         where translate(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),2), '-', '')  >=  '20170601'
           and a.order_status in (12,14,15,16) --退订
           and a.src_file_day between translate(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),2), '-', '') 
                              and translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),2)), '-', '')
           
         union all
         
        select a.sub_busi_id as sub_business_id
              ,a.region_id as order_msisdn_region_id
              ,a.region_id as payment_msisdn_region_id
              ,a.chn_id_new as channel_id
              ,b.user_id as order_user_id
          from cdmp_dw.td_aaa_order_log_d a 
          join rptdata.dim_userid_usernum b
            on a.serv_number = b.user_num
         where translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),2)), '-', '') <=  '20170531'
           and a.order_type = 2                 --退订
           and a.src_source_day between translate(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),2), '-', '') 
                                and translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),2)), '-', '')
       ) t
 group by sub_business_id
         ,order_msisdn_region_id
         ,payment_msisdn_region_id
         ,channel_id
         ,order_user_id
)
insert overwrite table stg.fact_after_2_mon_retention_user   --2个月后留存用户记录
select a.sub_business_id
      ,a.order_msisdn_region_id
      ,a.payment_msisdn_region_id
      ,a.channel_id
      ,a.order_user_id 
  from stg.fact_second_mon_retention_user a 
  left join after_2_mon_cancel_order_user b
    on (a.sub_business_id = b.sub_business_id
        and a.order_msisdn_region_id = b.order_msisdn_region_id 
        and a.payment_msisdn_region_id = b.payment_msisdn_region_id
        and a.channel_id = b.channel_id
        and a.order_user_id = b.order_user_id)
 where b.order_user_id is null 
 group by a.sub_business_id 
         ,a.order_msisdn_region_id
         ,a.payment_msisdn_region_id
         ,a.channel_id
         ,a.order_user_id;

=====================================================================================================================================

after_3_mon_cancel_order_user as (   --3个月后退订
select sub_business_id
      ,order_msisdn_region_id
      ,payment_msisdn_region_id
      ,channel_id
      ,order_user_id
  from (
        select a.sub_business_id
              ,a.order_msisdn_region_id
              ,a.payment_msisdn_region_id
              ,a.channel_id
              ,a.order_user_id
          from rptdata.fact_order_item_detail a 
         where translate(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),3), '-', '')  >=  '20170601'
           and a.order_status in (12,14,15,16) --退订
           and a.src_file_day between translate(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),3), '-', '') 
                              and translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),3)), '-', '')
           
         union all
         
        select a.sub_busi_id as sub_business_id
              ,a.region_id as order_msisdn_region_id
              ,a.region_id as payment_msisdn_region_id
              ,a.chn_id_new as channel_id
              ,b.user_id as order_user_id
          from cdmp_dw.td_aaa_order_log_d a 
          join rptdata.dim_userid_usernum b
            on a.serv_number = b.user_num
         where translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),3)), '-', '') <=  '20170531'
           and a.order_type = 2                 --退订
           and a.src_source_day between translate(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),3), '-', '') 
                                and translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),3)), '-', '')
       ) t
 group by sub_business_id
         ,order_msisdn_region_id
         ,payment_msisdn_region_id
         ,channel_id
         ,order_user_id
)
insert overwrite table stg.fact_after_3_mon_retention_user    --3个月后留存用户记录
select a.sub_business_id
      ,a.order_msisdn_region_id
      ,a.payment_msisdn_region_id
      ,a.channel_id
      ,a.order_user_id 
  from stg.fact_after_2_mon_cancel_order_user a 
  left join after_3_mon_cancel_order_user b
    on (a.sub_business_id = b.sub_business_id
        and a.order_msisdn_region_id = b.order_msisdn_region_id 
        and a.payment_msisdn_region_id = b.payment_msisdn_region_id
        and a.channel_id = b.channel_id
        and a.order_user_id = b.order_user_id)
 where b.order_user_id is null 
 group by a.sub_business_id 
         ,a.order_msisdn_region_id
         ,a.payment_msisdn_region_id
         ,a.channel_id
         ,a.order_user_id;

=====================================================================================================================================

after_4_mon_cancel_order_user as (   --4个月后退订
select sub_business_id
      ,order_msisdn_region_id
      ,payment_msisdn_region_id
      ,channel_id
      ,order_user_id
  from (
        select a.sub_business_id
              ,a.order_msisdn_region_id
              ,a.payment_msisdn_region_id
              ,a.channel_id
              ,a.order_user_id
          from rptdata.fact_order_item_detail a 
         where translate(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),4), '-', '')  >=  '20170601'
           and a.order_status in (12,14,15,16) --退订
           and a.src_file_day between translate(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),4), '-', '') 
                              and translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),4)), '-', '')
           
         union all
         
        select a.sub_busi_id as sub_business_id
              ,a.region_id as order_msisdn_region_id
              ,a.region_id as payment_msisdn_region_id
              ,a.chn_id_new as channel_id
              ,b.user_id as order_user_id
          from cdmp_dw.td_aaa_order_log_d a 
          join rptdata.dim_userid_usernum b
            on a.serv_number = b.user_num
         where translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),4)), '-', '') <=  '20170531'
           and a.order_type = 2                 --退订
           and a.src_source_day between translate(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),4), '-', '') 
                                and translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),4)), '-', '')
       ) t
 group by sub_business_id
         ,order_msisdn_region_id
         ,payment_msisdn_region_id
         ,channel_id
         ,order_user_id
)
insert overwrite table stg.fact_after_4_mon_retention_user    --4个月后留存用户记录
select a.sub_business_id
      ,a.order_msisdn_region_id
      ,a.payment_msisdn_region_id
      ,a.channel_id
      ,a.order_user_id 
  from stg.fact_after_3_mon_cancel_order_user a 
  left join after_4_mon_cancel_order_user b
    on (a.sub_business_id = b.sub_business_id
        and a.order_msisdn_region_id = b.order_msisdn_region_id 
        and a.payment_msisdn_region_id = b.payment_msisdn_region_id
        and a.channel_id = b.channel_id
        and a.order_user_id = b.order_user_id)
 where b.order_user_id is null 
 group by a.sub_business_id 
         ,a.order_msisdn_region_id
         ,a.payment_msisdn_region_id
         ,a.channel_id
         ,a.order_user_id;

=====================================================================================================================================

after_5_mon_cancel_order_user as (   --5个月后退订
select sub_business_id
      ,order_msisdn_region_id
      ,payment_msisdn_region_id
      ,channel_id
      ,order_user_id
  from (
        select a.sub_business_id
              ,a.order_msisdn_region_id
              ,a.payment_msisdn_region_id
              ,a.channel_id
              ,a.order_user_id
          from rptdata.fact_order_item_detail a 
         where translate(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),5), '-', '')  >=  '20170601'
           and a.order_status in (12,14,15,16) --退订
           and a.src_file_day between translate(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),5), '-', '') 
                              and translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),5)), '-', '')
           
         union all
         
        select a.sub_busi_id as sub_business_id
              ,a.region_id as order_msisdn_region_id
              ,a.region_id as payment_msisdn_region_id
              ,a.chn_id_new as channel_id
              ,b.user_id as order_user_id
          from cdmp_dw.td_aaa_order_log_d a 
          join rptdata.dim_userid_usernum b
            on a.serv_number = b.user_num
         where translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),5)), '-', '') <=  '20170531'
           and a.order_type = 2                 --退订
           and a.src_source_day between translate(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),5), '-', '') 
                                and translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),5)), '-', '')
       ) t
 group by sub_business_id
         ,order_msisdn_region_id
         ,payment_msisdn_region_id
         ,channel_id
         ,order_user_id                                                               
)
insert overwrite table stg.fact_after_5_mon_retention_user    --5个月后留存用户记录
select a.sub_business_id
      ,a.order_msisdn_region_id
      ,a.payment_msisdn_region_id
      ,a.channel_id
      ,a.order_user_id 
  from stg.fact_after_4_mon_cancel_order_user a 
  left join after_5_mon_cancel_order_user b
    on (a.sub_business_id = b.sub_business_id
        and a.order_msisdn_region_id = b.order_msisdn_region_id 
        and a.payment_msisdn_region_id = b.payment_msisdn_region_id
        and a.channel_id = b.channel_id
        and a.order_user_id = b.order_user_id)
 where b.order_user_id is null 
 group by a.sub_business_id 
         ,a.order_msisdn_region_id
         ,a.payment_msisdn_region_id
         ,a.channel_id
         ,a.order_user_id;

=====================================================================================================================================

after_6_mon_cancel_order_user as (   --6个月后退订
select sub_business_id
      ,order_msisdn_region_id
      ,payment_msisdn_region_id
      ,channel_id
      ,order_user_id
  from (
        select a.sub_business_id
              ,a.order_msisdn_region_id
              ,a.payment_msisdn_region_id
              ,a.channel_id
              ,a.order_user_id
          from rptdata.fact_order_item_detail a 
         where translate(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),6), '-', '')  >=  '20170601'
           and a.order_status in (12,14,15,16) --退订
           and a.src_file_day between translate(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),6), '-', '') 
                              and translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),6)), '-', '')
           
         union all
         
        select a.sub_busi_id as sub_business_id
              ,a.region_id as order_msisdn_region_id
              ,a.region_id as payment_msisdn_region_id
              ,a.chn_id_new as channel_id
              ,b.user_id as order_user_id
          from cdmp_dw.td_aaa_order_log_d a 
          join rptdata.dim_userid_usernum b
            on a.serv_number = b.user_num
         where translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),6)), '-', '') <=  '20170531'
           and a.order_type = 2                 --退订
           and a.src_source_day between translate(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),6), '-', '') 
                                and translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),6)), '-', '')
       ) t
 group by sub_business_id
         ,order_msisdn_region_id
         ,payment_msisdn_region_id
         ,channel_id
         ,order_user_id     
)
insert overwrite table stg.fact_after_6_mon_retention_user    --6个月后留存用户记录
select a.sub_business_id
      ,a.order_msisdn_region_id
      ,a.payment_msisdn_region_id
      ,a.channel_id
      ,a.order_user_id 
  from stg.fact_after_5_mon_cancel_order_user a 
  left join after_6_mon_cancel_order_user b
    on (a.sub_business_id = b.sub_business_id
        and a.order_msisdn_region_id = b.order_msisdn_region_id 
        and a.payment_msisdn_region_id = b.payment_msisdn_region_id
        and a.channel_id = b.channel_id
        and a.order_user_id = b.order_user_id)
 where b.order_user_id is null 
 group by a.sub_business_id 
         ,a.order_msisdn_region_id
         ,a.payment_msisdn_region_id
         ,a.channel_id
         ,a.order_user_id;



         
select t.sub_business_id
      ,t.order_msisdn_region_id
      ,t.payment_msisdn_region_id
      ,t.channel_id
      ,sum(this_mon_retention_user_cnt) as this_mon_retention_user_cnt
      ,sum(second_mon_retention_user_cnt) as second_mon_retention_user_cnt
      ,sum(after_2_mon_retention_user_cnt) as after_2_mon_retention_user_cnt
      ,sum(after_3_mon_retention_user_cnt) as after_3_mon_retention_user_cnt
      ,sum(after_4_mon_retention_user_cnt) as after_4_mon_retention_user_cnt
      ,sum(after_5_mon_retention_user_cnt) as after_5_mon_retention_user_cnt
      ,sum(after_6_mon_retention_user_cnt) as after_6_mon_retention_user_cnt     
  from (         
        select a.sub_business_id
              ,a.order_msisdn_region_id
              ,a.payment_msisdn_region_id
              ,a.channel_id
              ,count(a.order_user_id) as this_mon_retention_user_cnt
              ,0 as second_mon_retention_user_cnt
              ,0 as after_2_mon_retention_user_cnt
              ,0 as after_3_mon_retention_user_cnt
              ,0 as after_4_mon_retention_user_cnt
              ,0 as after_5_mon_retention_user_cnt
              ,0 as after_6_mon_retention_user_cnt
          from stg.fact_this_mon_retention_user a 
         group by a.sub_business_id
                 ,a.order_msisdn_region_id
                 ,a.payment_msisdn_region_id
                 ,a.channel_id
          
         union all  
                 
        select a.sub_business_id
              ,a.order_msisdn_region_id
              ,a.payment_msisdn_region_id
              ,a.channel_id
              ,0 as this_mon_retention_user_cnt
              ,count(a.order_user_id) as second_mon_retention_user_cnt
              ,0 as after_2_mon_retention_user_cnt
              ,0 as after_3_mon_retention_user_cnt
              ,0 as after_4_mon_retention_user_cnt
              ,0 as after_5_mon_retention_user_cnt
              ,0 as after_6_mon_retention_user_cnt
          from stg.fact_second_mon_retention_user a 
         group by a.sub_business_id
                 ,a.order_msisdn_region_id
                 ,a.payment_msisdn_region_id
                 ,a.channel_id

         union all  
                 
        select a.sub_business_id
              ,a.order_msisdn_region_id
              ,a.payment_msisdn_region_id
              ,a.channel_id
              ,0 as this_mon_retention_user_cnt
              ,0 as second_mon_retention_user_cnt
              ,count(a.order_user_id) as after_2_mon_retention_user_cnt
              ,0 as after_3_mon_retention_user_cnt
              ,0 as after_4_mon_retention_user_cnt
              ,0 as after_5_mon_retention_user_cnt
              ,0 as after_6_mon_retention_user_cnt
          from stg.fact_after_2_mon_retention_user a  
         group by a.sub_business_id
                 ,a.order_msisdn_region_id
                 ,a.payment_msisdn_region_id
                 ,a.channel_id
                 
         union all

        select a.sub_business_id
              ,a.order_msisdn_region_id
              ,a.payment_msisdn_region_id
              ,a.channel_id
              ,0 as this_mon_retention_user_cnt
              ,0 as second_mon_retention_user_cnt
              ,0 as after_2_mon_retention_user_cnt
              ,count(a.order_user_id) as after_3_mon_retention_user_cnt
              ,0 as after_4_mon_retention_user_cnt
              ,0 as after_5_mon_retention_user_cnt
              ,0 as after_6_mon_retention_user_cnt
          from stg.fact_after_3_mon_retention_user a 
         group by a.sub_business_id
                 ,a.order_msisdn_region_id
                 ,a.payment_msisdn_region_id
                 ,a.channel_id

         union all

        select a.sub_business_id
              ,a.order_msisdn_region_id
              ,a.payment_msisdn_region_id
              ,a.channel_id
              ,0 as this_mon_retention_user_cnt
              ,0 as second_mon_retention_user_cnt
              ,0 as after_2_mon_retention_user_cnt
              ,0 as after_3_mon_retention_user_cnt
              ,count(a.order_user_id) as after_4_mon_retention_user_cnt
              ,0 as after_5_mon_retention_user_cnt
              ,0 as after_6_mon_retention_user_cnt
          from stg.fact_after_4_mon_retention_user a 
         group by a.sub_business_id
                 ,a.order_msisdn_region_id
                 ,a.payment_msisdn_region_id
                 ,a.channel_id
          
         union all

        select a.sub_business_id
              ,a.order_msisdn_region_id
              ,a.payment_msisdn_region_id
              ,a.channel_id
              ,0 as this_mon_retention_user_cnt
              ,0 as second_mon_retention_user_cnt
              ,0 as after_2_mon_retention_user_cnt
              ,0 as after_3_mon_retention_user_cnt
              ,0 as after_4_mon_retention_user_cnt
              ,count(a.order_user_id) as after_5_mon_retention_user_cnt
              ,0 as after_6_mon_retention_user_cnt
          from stg.fact_after_5_mon_retention_user a 
         group by a.sub_business_id
                 ,a.order_msisdn_region_id
                 ,a.payment_msisdn_region_id
                 ,a.channel_id
                 
         union all

        select a.sub_business_id
              ,a.order_msisdn_region_id
              ,a.payment_msisdn_region_id
              ,a.channel_id
              ,0 as this_mon_retention_user_cnt
              ,0 as second_mon_retention_user_cnt
              ,0 as after_2_mon_retention_user_cnt
              ,0 as after_3_mon_retention_user_cnt
              ,0 as after_4_mon_retention_user_cnt
              ,0 as after_5_mon_retention_user_cnt
              ,count(a.order_user_id) as after_6_mon_retention_user_cnt
          from stg.fact_after_6_mon_retention_user a 
         group by a.sub_business_id
                 ,a.order_msisdn_region_id
                 ,a.payment_msisdn_region_id
                 ,a.channel_id
       ) t
 group by t.sub_business_id
         ,t.order_msisdn_region_id
         ,t.payment_msisdn_region_id
         ,t.channel_id
