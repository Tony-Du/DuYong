
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
 group by order_cancel_time    
         ,sub_business_id
         ,order_msisdn_region_id
         ,payment_msisdn_region_id
         ,channel_id
         ,order_user_id           
),
this_mon_retention_user as (   --当月留存用户记录
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
 where b.order_user_id is null
 group by a.sub_business_id
         ,a.order_msisdn_region_id
         ,a.payment_msisdn_region_id
         ,a.channel_id
         ,a.order_user_id
),

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
),
second_mon_retention_user as (   --次月留存用户记录
select a.sub_business_id
      ,a.order_msisdn_region_id
      ,a.payment_msisdn_region_id
      ,a.channel_id
      ,a.order_user_id 
  from this_mon_retention_user a
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
         ,a.order_user_id
),

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
),
after_2_mon_retention_user as (  --2个月后留存用户记录
select a.sub_business_id
      ,a.order_msisdn_region_id
      ,a.payment_msisdn_region_id
      ,a.channel_id
      ,a.order_user_id 
  from second_mon_retention_user a 
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
         ,a.order_user_id
),

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
),
after_3_mon_retention_user as (   --3个月后留存用户记录
select a.sub_business_id
      ,a.order_msisdn_region_id
      ,a.payment_msisdn_region_id
      ,a.channel_id
      ,a.order_user_id 
  from after_2_mon_retention_user a 
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
         ,a.order_user_id
),

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
),
after_4_mon_retention_user as (   --4个月后留存用户记录
select a.sub_business_id
      ,a.order_msisdn_region_id
      ,a.payment_msisdn_region_id
      ,a.channel_id
      ,a.order_user_id 
  from after_3_mon_retention_user a 
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
         ,a.order_user_id
),
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
),
after_5_mon_retention_user as (    --5个月后留存用户记录
select a.sub_business_id
      ,a.order_msisdn_region_id
      ,a.payment_msisdn_region_id
      ,a.channel_id
      ,a.order_user_id 
  from after_4_mon_retention_user a 
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
         ,a.order_user_id
),
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
),
after_6_mon_retention_user as (    --6个月后留存用户记录
select a.sub_business_id
      ,a.order_msisdn_region_id
      ,a.payment_msisdn_region_id
      ,a.channel_id
      ,a.order_user_id 
  from after_5_mon_retention_user a 
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
         ,a.order_user_id
) 
select '${EXTRACT_MONTH}' as statis_month                  --新增订购月份
      ,nvl(business_type_name, '剔重汇总') as business_type_name
      ,nvl(business_name, '剔重汇总') as business_name
      ,nvl(sub_business_name, '剔重汇总') as sub_business_name
      ,nvl(phone_province_name, '剔重汇总') as phone_province_name
      ,nvl(channel_id, '剔重汇总') as channel_id
      ,nvl(chn_name, '剔重汇总') as chn_name
      ,nvl(chn_attr_1_name, '剔重汇总') as chn_attr_1_name
      ,nvl(chn_attr_2_name, '剔重汇总') as chn_attr_2_name
      ,sum(new_add_order_user_cnt) as new_add_order_user_cnt
      ,sum(this_mon_retention_user_cnt) as this_mon_retention_user_cnt
      ,sum(second_mon_retention_user_cnt) as second_mon_retention_user_cnt
      ,sum(after_2_mon_retention_user_cnt) as after_2_mon_retention_user_cnt
      ,sum(after_3_mon_retention_user_cnt) as after_3_mon_retention_user_cnt
      ,sum(after_4_mon_retention_user_cnt) as after_4_mon_retention_user_cnt
      ,sum(after_5_mon_retention_user_cnt) as after_5_mon_retention_user_cnt
      ,sum(after_6_mon_retention_user_cnt) as after_6_mon_retention_user_cnt      
  from (
        select nvl(h.business_type_name, 'NA') as business_type_name                             
              ,nvl(h.business_name, 'NA') as business_name                      
              ,nvl(h.sub_busi_name, 'NA') as sub_business_name                         
              ,nvl(i.prov_name, 'NA') as phone_province_name  
              ,a.channel_id                                                       
              ,nvl(g.chn_name, 'NA') as chn_name                                
              ,nvl(g.chn_attr_1_name, 'NA') as chn_attr_1_name                  
              ,nvl(g.chn_attr_2_name, 'NA') as chn_attr_2_name                  
              ,count(a.order_user_id) as new_add_order_user_cnt   --新增订购用户数
              ,0 as this_mon_retention_user_cnt
              ,0 as second_mon_retention_user_cnt
              ,0 as after_2_mon_retention_user_cnt
              ,0 as after_3_mon_retention_user_cnt
              ,0 as after_4_mon_retention_user_cnt
              ,0 as after_5_mon_retention_user_cnt
              ,0 as after_6_mon_retention_user_cnt 
          from new_add_order_user a
          left join rptdata.dim_sub_busi h
            on (case when a.sub_business_id = '-998' then concat('', rand()) else a.sub_business_id end) = h.sub_busi_id
          left join rptdata.dim_region i
            on (case when a.order_msisdn_region_id = '-998' then (case when a.payment_msisdn_region_id = '-998' then concat('', rand()) else a.payment_msisdn_region_id end) else a.order_msisdn_region_id end) = i.region_id
          left join rptdata.dim_chn g
            on (case when a.channel_id = '-998' then concat('', rand()) else a.channel_id end) = g.chn_id 
         group by nvl(h.business_type_name, 'NA')     
                 ,nvl(h.business_name, 'NA')                    
                 ,nvl(h.sub_busi_name, 'NA')                        
                 ,nvl(i.prov_name, 'NA')  
                 ,a.channel_id                                                       
                 ,nvl(g.chn_name, 'NA')                           
                 ,nvl(g.chn_attr_1_name, 'NA')               
                 ,nvl(g.chn_attr_2_name, 'NA')
                          
         union all 

        select nvl(h.business_type_name, 'NA') as business_type_name                             --业务类型
              ,nvl(h.business_name, 'NA') as business_name                      --业务名称
              ,nvl(h.sub_busi_name, 'NA') as sub_business_name                  --子业务名称       
              ,nvl(i.prov_name, 'NA') as phone_province_name 
              ,a.channel_id                                                     --渠道id      
              ,nvl(g.chn_name, 'NA') as chn_name                                --渠道名称
              ,nvl(g.chn_attr_1_name, 'NA') as chn_attr_1_name                  --渠道一级
              ,nvl(g.chn_attr_2_name, 'NA') as chn_attr_2_name                  --渠道二级
              ,0 as new_add_order_user_cnt
              ,count(a.order_user_id) as this_mon_retention_user_cnt
              ,0 as second_mon_retention_user_cnt
              ,0 as after_2_mon_retention_user_cnt
              ,0 as after_3_mon_retention_user_cnt
              ,0 as after_4_mon_retention_user_cnt
              ,0 as after_5_mon_retention_user_cnt
              ,0 as after_6_mon_retention_user_cnt 
          from this_mon_retention_user a
          left join rptdata.dim_sub_busi h
            on (case when a.sub_business_id = '-998' then concat('', rand()) else a.sub_business_id end) = h.sub_busi_id
          left join rptdata.dim_region i
            on (case when a.order_msisdn_region_id = '-998' then (case when a.payment_msisdn_region_id = '-998' then concat('', rand()) else a.payment_msisdn_region_id end) else a.order_msisdn_region_id end) = i.region_id
          left join rptdata.dim_chn g
            on (case when a.channel_id = '-998' then concat('', rand()) else a.channel_id end) = g.chn_id 
         group by nvl(h.business_type_name, 'NA')     
                 ,nvl(h.business_name, 'NA')                    
                 ,nvl(h.sub_busi_name, 'NA')                        
                 ,nvl(i.prov_name, 'NA')  
                 ,a.channel_id                                                       
                 ,nvl(g.chn_name, 'NA')                           
                 ,nvl(g.chn_attr_1_name, 'NA')               
                 ,nvl(g.chn_attr_2_name, 'NA')          
          
         union all 

        select nvl(h.business_type_name, 'NA') as business_type_name                             --业务类型
              ,nvl(h.business_name, 'NA') as business_name                      --业务名称
              ,nvl(h.sub_busi_name, 'NA') as sub_business_name                  --子业务名称       
              ,nvl(i.prov_name, 'NA') as phone_province_name 
              ,a.channel_id                                                     --渠道id      
              ,nvl(g.chn_name, 'NA') as chn_name                                --渠道名称
              ,nvl(g.chn_attr_1_name, 'NA') as chn_attr_1_name                  --渠道一级
              ,nvl(g.chn_attr_2_name, 'NA') as chn_attr_2_name                  --渠道二级
              ,0 as new_add_order_user_cnt
              ,0 as this_mon_retention_user_cnt
              ,count(a.order_user_id) as second_mon_retention_user_cnt
              ,0 as after_2_mon_retention_user_cnt
              ,0 as after_3_mon_retention_user_cnt
              ,0 as after_4_mon_retention_user_cnt
              ,0 as after_5_mon_retention_user_cnt
              ,0 as after_6_mon_retention_user_cnt 
          from second_mon_retention_user a
          left join rptdata.dim_sub_busi h
            on (case when a.sub_business_id = '-998' then concat('', rand()) else a.sub_business_id end) = h.sub_busi_id
          left join rptdata.dim_region i
            on (case when a.order_msisdn_region_id = '-998' then (case when a.payment_msisdn_region_id = '-998' then concat('', rand()) else a.payment_msisdn_region_id end) else a.order_msisdn_region_id end) = i.region_id
          left join rptdata.dim_chn g
            on (case when a.channel_id = '-998' then concat('', rand()) else a.channel_id end) = g.chn_id 
         group by nvl(h.business_type_name, 'NA')     
                 ,nvl(h.business_name, 'NA')                    
                 ,nvl(h.sub_busi_name, 'NA')                        
                 ,nvl(i.prov_name, 'NA')  
                 ,a.channel_id                                                       
                 ,nvl(g.chn_name, 'NA')                           
                 ,nvl(g.chn_attr_1_name, 'NA')               
                 ,nvl(g.chn_attr_2_name, 'NA')

         union all 

        select nvl(h.business_type_name, 'NA') as business_type_name                             --业务类型
              ,nvl(h.business_name, 'NA') as business_name                      --业务名称
              ,nvl(h.sub_busi_name, 'NA') as sub_business_name                  --子业务名称       
              ,nvl(i.prov_name, 'NA') as phone_province_name 
              ,a.channel_id                                                     --渠道id      
              ,nvl(g.chn_name, 'NA') as chn_name                                --渠道名称
              ,nvl(g.chn_attr_1_name, 'NA') as chn_attr_1_name                  --渠道一级
              ,nvl(g.chn_attr_2_name, 'NA') as chn_attr_2_name                  --渠道二级
              ,0 as new_add_order_user_cnt
              ,0 as this_mon_retention_user_cnt
              ,0 as second_mon_retention_user_cnt
              ,count(a.order_user_id) as after_2_mon_retention_user_cnt
              ,0 as after_3_mon_retention_user_cnt
              ,0 as after_4_mon_retention_user_cnt
              ,0 as after_5_mon_retention_user_cnt
              ,0 as after_6_mon_retention_user_cnt 
          from after_2_mon_retention_user a
          left join rptdata.dim_sub_busi h
            on (case when a.sub_business_id = '-998' then concat('', rand()) else a.sub_business_id end) = h.sub_busi_id
          left join rptdata.dim_region i
            on (case when a.order_msisdn_region_id = '-998' then (case when a.payment_msisdn_region_id = '-998' then concat('', rand()) else a.payment_msisdn_region_id end) else a.order_msisdn_region_id end) = i.region_id
          left join rptdata.dim_chn g
            on (case when a.channel_id = '-998' then concat('', rand()) else a.channel_id end) = g.chn_id 
         group by nvl(h.business_type_name, 'NA')     
                 ,nvl(h.business_name, 'NA')                    
                 ,nvl(h.sub_busi_name, 'NA')                        
                 ,nvl(i.prov_name, 'NA')  
                 ,a.channel_id                                                       
                 ,nvl(g.chn_name, 'NA')                           
                 ,nvl(g.chn_attr_1_name, 'NA')               
                 ,nvl(g.chn_attr_2_name, 'NA')

         union all 

        select nvl(h.business_type_name, 'NA') as business_type_name                             --业务类型
              ,nvl(h.business_name, 'NA') as business_name                      --业务名称
              ,nvl(h.sub_busi_name, 'NA') as sub_business_name                  --子业务名称       
              ,nvl(i.prov_name, 'NA') as phone_province_name 
              ,a.channel_id                                                     --渠道id      
              ,nvl(g.chn_name, 'NA') as chn_name                                --渠道名称
              ,nvl(g.chn_attr_1_name, 'NA') as chn_attr_1_name                  --渠道一级
              ,nvl(g.chn_attr_2_name, 'NA') as chn_attr_2_name                  --渠道二级
              ,0 as new_add_order_user_cnt
              ,0 as this_mon_retention_user_cnt
              ,0 as second_mon_retention_user_cnt
              ,0 as after_2_mon_retention_user_cnt
              ,count(a.order_user_id) as after_3_mon_retention_user_cnt
              ,0 as after_4_mon_retention_user_cnt
              ,0 as after_5_mon_retention_user_cnt
              ,0 as after_6_mon_retention_user_cnt 
          from after_3_mon_retention_user a
          left join rptdata.dim_sub_busi h
            on (case when a.sub_business_id = '-998' then concat('', rand()) else a.sub_business_id end) = h.sub_busi_id
          left join rptdata.dim_region i
            on (case when a.order_msisdn_region_id = '-998' then (case when a.payment_msisdn_region_id = '-998' then concat('', rand()) else a.payment_msisdn_region_id end) else a.order_msisdn_region_id end) = i.region_id
          left join rptdata.dim_chn g
            on (case when a.channel_id = '-998' then concat('', rand()) else a.channel_id end) = g.chn_id 
         group by nvl(h.business_type_name, 'NA')     
                 ,nvl(h.business_name, 'NA')                    
                 ,nvl(h.sub_busi_name, 'NA')                        
                 ,nvl(i.prov_name, 'NA')  
                 ,a.channel_id                                                       
                 ,nvl(g.chn_name, 'NA')                           
                 ,nvl(g.chn_attr_1_name, 'NA')               
                 ,nvl(g.chn_attr_2_name, 'NA')

         union all 

        select nvl(h.business_type_name, 'NA') as business_type_name                             --业务类型
              ,nvl(h.business_name, 'NA') as business_name                      --业务名称
              ,nvl(h.sub_busi_name, 'NA') as sub_business_name                  --子业务名称       
              ,nvl(i.prov_name, 'NA') as phone_province_name 
              ,a.channel_id                                                     --渠道id      
              ,nvl(g.chn_name, 'NA') as chn_name                                --渠道名称
              ,nvl(g.chn_attr_1_name, 'NA') as chn_attr_1_name                  --渠道一级
              ,nvl(g.chn_attr_2_name, 'NA') as chn_attr_2_name                  --渠道二级
              ,0 as new_add_order_user_cnt
              ,0 as this_mon_retention_user_cnt
              ,0 as second_mon_retention_user_cnt
              ,0 as after_2_mon_retention_user_cnt
              ,0 as after_3_mon_retention_user_cnt
              ,count(a.order_user_id) as after_4_mon_retention_user_cnt
              ,0 as after_5_mon_retention_user_cnt
              ,0 as after_6_mon_retention_user_cnt 
          from after_4_mon_retention_user a
          left join rptdata.dim_sub_busi h
            on (case when a.sub_business_id = '-998' then concat('', rand()) else a.sub_business_id end) = h.sub_busi_id
          left join rptdata.dim_region i
            on (case when a.order_msisdn_region_id = '-998' then (case when a.payment_msisdn_region_id = '-998' then concat('', rand()) else a.payment_msisdn_region_id end) else a.order_msisdn_region_id end) = i.region_id
          left join rptdata.dim_chn g
            on (case when a.channel_id = '-998' then concat('', rand()) else a.channel_id end) = g.chn_id 
         group by nvl(h.business_type_name, 'NA')     
                 ,nvl(h.business_name, 'NA')                    
                 ,nvl(h.sub_busi_name, 'NA')                        
                 ,nvl(i.prov_name, 'NA')  
                 ,a.channel_id                                                       
                 ,nvl(g.chn_name, 'NA')                           
                 ,nvl(g.chn_attr_1_name, 'NA')               
                 ,nvl(g.chn_attr_2_name, 'NA')

         union all 

        select nvl(h.business_type_name, 'NA') as business_type_name                             --业务类型
              ,nvl(h.business_name, 'NA') as business_name                      --业务名称
              ,nvl(h.sub_busi_name, 'NA') as sub_business_name                  --子业务名称       
              ,nvl(i.prov_name, 'NA') as phone_province_name 
              ,a.channel_id                                                     --渠道id      
              ,nvl(g.chn_name, 'NA') as chn_name                                --渠道名称
              ,nvl(g.chn_attr_1_name, 'NA') as chn_attr_1_name                  --渠道一级
              ,nvl(g.chn_attr_2_name, 'NA') as chn_attr_2_name                  --渠道二级
              ,0 as new_add_order_user_cnt
              ,0 as this_mon_retention_user_cnt
              ,0 as second_mon_retention_user_cnt
              ,0 as after_2_mon_retention_user_cnt
              ,0 as after_3_mon_retention_user_cnt
              ,0 as after_4_mon_retention_user_cnt
              ,count(a.order_user_id) as after_5_mon_retention_user_cnt
              ,0 as after_6_mon_retention_user_cnt 
          from after_5_mon_retention_user a
          left join rptdata.dim_sub_busi h
            on (case when a.sub_business_id = '-998' then concat('', rand()) else a.sub_business_id end) = h.sub_busi_id
          left join rptdata.dim_region i
            on (case when a.order_msisdn_region_id = '-998' then (case when a.payment_msisdn_region_id = '-998' then concat('', rand()) else a.payment_msisdn_region_id end) else a.order_msisdn_region_id end) = i.region_id
          left join rptdata.dim_chn g
            on (case when a.channel_id = '-998' then concat('', rand()) else a.channel_id end) = g.chn_id 
         group by nvl(h.business_type_name, 'NA')     
                 ,nvl(h.business_name, 'NA')                    
                 ,nvl(h.sub_busi_name, 'NA')                        
                 ,nvl(i.prov_name, 'NA')  
                 ,a.channel_id                                                       
                 ,nvl(g.chn_name, 'NA')                           
                 ,nvl(g.chn_attr_1_name, 'NA')               
                 ,nvl(g.chn_attr_2_name, 'NA')

         union all 

        select nvl(h.business_type_name, 'NA') as business_type_name                             --业务类型
              ,nvl(h.business_name, 'NA') as business_name                      --业务名称
              ,nvl(h.sub_busi_name, 'NA') as sub_business_name                  --子业务名称       
              ,nvl(i.prov_name, 'NA') as phone_province_name 
              ,a.channel_id                                                     --渠道id      
              ,nvl(g.chn_name, 'NA') as chn_name                                --渠道名称
              ,nvl(g.chn_attr_1_name, 'NA') as chn_attr_1_name                  --渠道一级
              ,nvl(g.chn_attr_2_name, 'NA') as chn_attr_2_name                  --渠道二级
              ,0 as new_add_order_user_cnt 
              ,0 as this_mon_retention_user_cnt
              ,0 as second_mon_retention_user_cnt
              ,0 as after_2_mon_retention_user_cnt
              ,0 as after_3_mon_retention_user_cnt
              ,0 as after_4_mon_retention_user_cnt
              ,0 as after_5_mon_retention_user_cnt
              ,count(a.order_user_id) as after_6_mon_retention_user_cnt
          from after_6_mon_retention_user a
          left join rptdata.dim_sub_busi h
            on (case when a.sub_business_id = '-998' then concat('', rand()) else a.sub_business_id end) = h.sub_busi_id
          left join rptdata.dim_region i
            on (case when a.order_msisdn_region_id = '-998' then (case when a.payment_msisdn_region_id = '-998' then concat('', rand()) else a.payment_msisdn_region_id end) else a.order_msisdn_region_id end) = i.region_id
          left join rptdata.dim_chn g
            on (case when a.channel_id = '-998' then concat('', rand()) else a.channel_id end) = g.chn_id 
         group by nvl(h.business_type_name, 'NA')     
                 ,nvl(h.business_name, 'NA')                    
                 ,nvl(h.sub_busi_name, 'NA')                        
                 ,nvl(i.prov_name, 'NA')  
                 ,a.channel_id                                                       
                 ,nvl(g.chn_name, 'NA')                           
                 ,nvl(g.chn_attr_1_name, 'NA')               
                 ,nvl(g.chn_attr_2_name, 'NA')            
       ) t
 group by business_type_name,business_name,sub_business_name,channel_id
         ,chn_name,chn_attr_1_name,chn_attr_2_name,phone_province_name   
grouping sets (
(business_type_name, business_name, sub_business_name, channel_id, chn_name, chn_attr_1_name, chn_attr_2_name),
(business_type_name, business_name, sub_business_name, channel_id, chn_name, chn_attr_1_name, chn_attr_2_name, phone_province_name)
);                  

