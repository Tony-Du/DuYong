
with tmp_new_add_order_user as( --当前周期(如：456份，三个月)新增
select a.sub_business_id
      ,a.order_msisdn_region_id
      ,a.payment_msisdn_region_id
      ,a.channel_id
      ,a.order_user_id
  from rptdata.fact_order_item_detail a 
 where a.src_file_day >= '20170601'   
   and a.src_file_day between '${EXTRACT_START_DAY}' and '${EXTRACT_END_DAY}'  
   and a.order_status in (5,9) 
   
 union all 

select a.sub_busi_id as sub_business_id
      ,a.region_id as order_msisdn_region_id
      ,a.region_id as payment_msisdn_region_id
      ,a.chn_id_new as channel_id
      ,b.user_id as order_user_id
  from cdmp_dw.td_aaa_order_log_d a 
  join rptdata.dim_userid_usernum b
    on a.serv_number = b.user_num
 where a.src_source_day <= '20170531'
   and a.src_source_day between '${EXTRACT_START_DAY}' and '${EXTRACT_END_DAY}'
   and a.order_type = 1   
   
),
tmp_one_day_in_order_user as(  --某一天在订
select a.sub_business_id
      ,a.order_msisdn_region_id
      ,a.payment_msisdn_region_id
      ,a.channel_id
      ,a.order_user_id
  from rptdata.fact_order_daily_snapshot a 
 where '${RETENTION_DAY}' >= '20170601'
   and a.snapshot_day = '${RETENTION_DAY}'
 
 union all
 
select a.sub_busi_id as sub_business_id
      ,a.region_id as order_msisdn_region_id
      ,a.region_id as payment_msisdn_region_id
      ,a.chn_id_new as channel_id
      ,b.user_id as order_user_id
  from cdmp_dw.td_aaa_order_d a 
  join rptdata.dim_userid_usernum b
    on a.serv_number = b.user_num
 where '${RETENTION_DAY}' <= '20170531'
   and a.src_source_day = '${RETENTION_DAY}' 
)
select  substr('${EXTRACT_START_DAY}',1,6) as statis_start_month                  --新增订购周期的第一个月份
       ,substr('${EXTRACT_END_DAY}',1,6) as statis_end_month                      --新增订购周期的最后一个月份
       ,nvl(business_type, '剔重汇总') as business_type
       ,nvl(business_name, '剔重汇总') as business_name
       ,nvl(sub_business_name, '剔重汇总') as sub_business_name
       ,nvl(phone_province_name, '剔重汇总') as phone_province_name
       ,nvl(channel_id, '剔重汇总') as channel_id
       ,nvl(chn_name, '剔重汇总') as chn_name
       ,nvl(chn_attr_1_name, '剔重汇总') as chn_attr_1_name
       ,nvl(chn_attr_2_name, '剔重汇总') as chn_attr_2_name
       ,count(distinct new_add_order_user_id) as new_add_order_user_cnt                 --新增订购用户数      
       ,count(distinct one_day_retention_user_id) as one_day_retention_user_cnt         --某一天的留存用户数  
       ,grouping__id as grain_ind
  from ( 
        select nvl(h.b_type, 'NA') as business_type                             --业务类型
              ,nvl(h.business_name, 'NA') as business_name                      --业务名称
              ,nvl(h.sub_busi_name, 'NA') as sub_business_name                  --子业务名称       
              ,nvl(i.prov_name, nvl(j.prov_name, 'NA')) as phone_province_name  --省份
              ,a.channel_id                                                     --渠道id      
              ,nvl(g.chn_name, 'NA') as chn_name                                --渠道名称
              ,nvl(g.chn_attr_1_name, 'NA') as chn_attr_1_name                  --渠道一级
              ,nvl(g.chn_attr_2_name, 'NA') as chn_attr_2_name                  --渠道二级
              ,a.order_user_id as new_add_order_user_id
              ,d1.order_user_id as one_day_retention_user_id     
          from tmp_new_add_order_user a          
          left join tmp_one_day_in_order_user d1 
            on a.order_user_id = d1.order_user_id 
          left join rptdata.dim_server h
            on (case when a.sub_business_id = '-998' then concat('', rand()) else a.sub_business_id end) = h.sub_busi_id
          left join rptdata.dim_region i
            on (case when a.order_msisdn_region_id = '-998' then concat('', rand()) else a.order_msisdn_region_id end) = i.region_id
          left join rptdata.dim_region j
            on (case when a.payment_msisdn_region_id = '-998' then concat('', rand()) else a.payment_msisdn_region_id end) = j.region_id  
          left join rptdata.dim_chn g
            on (case when a.channel_id = '-998' then concat('', rand()) else a.channel_id end) = g.chn_id 
       ) t 
 group by business_type,business_name,sub_business_name,channel_id
         ,chn_name,chn_attr_1_name,chn_attr_2_name,phone_province_name   
grouping sets (
(business_type, business_name, sub_business_name, channel_id, chn_name, chn_attr_1_name, chn_attr_2_name),
(business_type, business_name, sub_business_name, channel_id, chn_name, chn_attr_1_name, chn_attr_2_name, phone_province_name)
); 


