



--退订用户数	统计周期内，成功退续订业务的用户数。维度：部门、业务、终端、省份、地市、渠道、付费渠道、货币类型、是否虚拟局数据、订购周期、订购周期单位、是否自动续订	

--count(distinct order_user_id) where src_file_day between xxx and order_status in (12,14,15,16)	

--rptdata.fact_order_item_detail

--新增订购用户数	统计周期内，成功订购业务的用户数。维度：部门、业务、终端、省份、地市、渠道、付费渠道、货币类型、是否虚拟局数据、订购周期、订购周期单位、是否自动续订

--count(distinct order_user_id) where src_file_day between xxx and order_status in (5,9)	

--rptdata.fact_order_item_detail

--5月31日之前新增/退订的用户订单
--cdmp_dw.td_aaa_order_log_d order_type = 1新增 order_type = 2退订



with new_add_order_user as( --当月新增
select a.sub_business_id
      ,a.order_msisdn_region_id
      ,a.payment_msisdn_region_id
      ,a.channel_id
      ,a.order_user_id
  from rptdata.fact_order_item_detail a 
 where '${EXTRACT_MONTH}' >=  '201706'
   and a.order_status in (5,9)   --新增 flag
   and a.src_file_day between '${EXTRACT_MONTH}01' and '${EXTRACT_MONTH}31'
   
 union all
 
select a.sub_busi_id as sub_business_id
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
),


this_mon_cancel_order_user as (   --当月退订
select a.sub_business_id
      ,a.order_msisdn_region_id
      ,a.payment_msisdn_region_id
      ,a.channel_id
      ,a.order_user_id
  from rptdata.fact_order_item_detail a 
 where '${EXTRACT_MONTH}' >=  '201706'
   and a.order_status in (12,14,15,16) --退订 flag
   and a.src_file_day between '${EXTRACT_MONTH}01' and '${EXTRACT_MONTH}31'
   
 union all
 
select a.sub_busi_id as sub_business_id
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
),


second_mon_cancel_order_user as (   --次月退订
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
),

--translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),1)), '-', '')   --次月最后一天

---translate(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),1), '-', '')  --次月第一天


after_2_mon_cancel_order_user as (   --2个月后退订
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
),

after_3_mon_cancel_order_user as (   --3个月后退订
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
),

after_4_mon_cancel_order_user as (   --4个月后退订
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
),

after_5_mon_cancel_order_user as (   --5个月后退订
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
),

after_6_mon_cancel_order_user as (   --6个月后退订
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
)





  

