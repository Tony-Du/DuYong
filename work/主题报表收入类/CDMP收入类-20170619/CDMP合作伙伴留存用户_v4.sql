
--create view rpt.fact_cdmp_cooperation_partner_retention_user_v as

with tmp_new_add_order_user as( --当月新增
select  sub_business_id                       --优化一：在子查询中剔重
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
         where '${EXTRACT_MONTH}' >=  '201706'
           and a.order_status in (5,9) 
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
           and a.order_type = 1 
           and a.src_source_day between '${EXTRACT_MONTH}01' and '${EXTRACT_MONTH}31' 
       ) t
 group by sub_business_id
         ,order_msisdn_region_id
         ,payment_msisdn_region_id
         ,channel_id
         ,order_user_id            
),


tmp_this_mon_in_order_user as(     --优化2：指标单个进行计算
select t1.sub_business_id
      ,t1.order_msisdn_region_id
      ,t1.payment_msisdn_region_id
      ,t1.channel_id
      ,count(t2.order_user_id) as this_mon_retention_user_cnt  --当月留存用户数
  from tmp_new_add_order_user t1        
  left join (                           --当月最后一天在订
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
                       from rptdata.fact_order_daily_snapshot a 
                      where translate(last_day(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01')), '-', '') >= '20170601'
                        and a.snapshot_day = translate(last_day(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01')), '-', '')
                      
                      union all
                      
                     select a.sub_busi_id as sub_business_id 
                           ,a.region_id as order_msisdn_region_id 
                           ,a.region_id as payment_msisdn_region_id 
                           ,a.chn_id_new as channel_id 
                           ,b.user_id as order_user_id 
                       from cdmp_dw.td_aaa_order_d a  
                       join rptdata.dim_userid_usernum b 
                         on a.serv_number = b.user_num
                      where translate(last_day(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01')), '-', '') <= '20170531'
                        and a.src_source_day = translate(last_day(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01')), '-', '')
                    ) t
              group by sub_business_id
                      ,order_msisdn_region_id
                      ,payment_msisdn_region_id
                      ,channel_id
                      ,order_user_id    
           ) t2 
    on (t1.sub_business_id = t2.sub_business_id and t1.order_msisdn_region_id = t2.order_msisdn_region_id 
        and t1.payment_msisdn_region_id = t2.payment_msisdn_region_id and t1.channel_id = t2.channel_id and t1.order_user_id = t2.order_user_id)
 group by t1.sub_business_id
         ,t1.order_msisdn_region_id
         ,t1.payment_msisdn_region_id
         ,t1.channel_id              
),


tmp_second_mon_in_order_user as(  
select t1.sub_business_id
      ,t1.order_msisdn_region_id
      ,t1.payment_msisdn_region_id
      ,t1.channel_id
      ,count(t2.order_user_id) as second_mon_retention_user_cnt  --次月留存用户数  
  from tmp_new_add_order_user t1        
  left join (                           --次月最后一天在订
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
                      from rptdata.fact_order_daily_snapshot a 
                     where translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),1)), '-', '') >= '20170601'
                       and a.snapshot_day = translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),1)), '-', '')
                     
                     union all
                     
                    select a.sub_busi_id as sub_business_id
                          ,a.region_id as order_msisdn_region_id
                          ,a.region_id as payment_msisdn_region_id
                          ,a.chn_id_new as channel_id
                          ,b.user_id as order_user_id
                      from cdmp_dw.td_aaa_order_d a 
                      join rptdata.dim_userid_usernum b
                        on a.serv_number = b.user_num
                     where translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),1)), '-', '') <= '20170531'
                       and a.src_source_day = translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),1)), '-', '')
                    ) t
              group by sub_business_id
                      ,order_msisdn_region_id
                      ,payment_msisdn_region_id
                      ,channel_id
                      ,order_user_id    
           ) t2 
    on (t1.sub_business_id = t2.sub_business_id and t1.order_msisdn_region_id = t2.order_msisdn_region_id 
        and t1.payment_msisdn_region_id = t2.payment_msisdn_region_id and t1.channel_id = t2.channel_id and t1.order_user_id = t2.order_user_id)    
 group by t1.sub_business_id
         ,t1.order_msisdn_region_id
         ,t1.payment_msisdn_region_id
         ,t1.channel_id   
),  


tmp_after_2_mon_in_order_user as( 
select t1.sub_business_id
      ,t1.order_msisdn_region_id
      ,t1.payment_msisdn_region_id
      ,t1.channel_id
      ,count(t2.order_user_id) as after_2_mon_retention_user_cnt  --2个月留存用户数 
  from tmp_new_add_order_user t1        
  left join (                           --2个月最后一天在订
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
                      from rptdata.fact_order_daily_snapshot a 
                     where translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),2)), '-', '') >= '20170601'
                       and a.snapshot_day = translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),2)), '-', '')
                     
                     union all
                     
                    select a.sub_busi_id as sub_business_id
                          ,a.region_id as order_msisdn_region_id
                          ,a.region_id as payment_msisdn_region_id
                          ,a.chn_id_new as channel_id
                          ,b.user_id as order_user_id
                      from cdmp_dw.td_aaa_order_d a 
                      join rptdata.dim_userid_usernum b
                        on a.serv_number = b.user_num
                     where translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),2)), '-', '') <= '20170531'
                       and a.src_source_day = translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),2)), '-', '') 
                    ) t
              group by sub_business_id
                      ,order_msisdn_region_id
                      ,payment_msisdn_region_id
                      ,channel_id
                      ,order_user_id    
           ) t2 
    on (t1.sub_business_id = t2.sub_business_id and t1.order_msisdn_region_id = t2.order_msisdn_region_id 
        and t1.payment_msisdn_region_id = t2.payment_msisdn_region_id and t1.channel_id = t2.channel_id and t1.order_user_id = t2.order_user_id)     
 group by t1.sub_business_id
         ,t1.order_msisdn_region_id
         ,t1.payment_msisdn_region_id
         ,t1.channel_id  
), 


tmp_after_3_mon_in_order_user as( 
select t1.sub_business_id
      ,t1.order_msisdn_region_id
      ,t1.payment_msisdn_region_id
      ,t1.channel_id
      ,count(t2.order_user_id) as after_3_mon_retention_user_cnt  --3个月留存用户数 
  from tmp_new_add_order_user t1        
  left join (                           --3个月最后一天在订
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
                      from rptdata.fact_order_daily_snapshot a 
                     where translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),3)), '-', '') >= '20170601'
                       and a.snapshot_day = translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),3)), '-', '')
                     
                     union all
                     
                    select a.sub_busi_id as sub_business_id
                          ,a.region_id as order_msisdn_region_id
                          ,a.region_id as payment_msisdn_region_id
                          ,a.chn_id_new as channel_id
                          ,b.user_id as order_user_id
                      from cdmp_dw.td_aaa_order_d a 
                      join rptdata.dim_userid_usernum b
                        on a.serv_number = b.user_num
                     where translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),3)), '-', '') <= '20170531'
                       and a.src_source_day = translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),3)), '-', '') 
                    ) t
              group by sub_business_id
                      ,order_msisdn_region_id
                      ,payment_msisdn_region_id
                      ,channel_id
                      ,order_user_id    
           ) t2 
    on (t1.sub_business_id = t2.sub_business_id and t1.order_msisdn_region_id = t2.order_msisdn_region_id 
        and t1.payment_msisdn_region_id = t2.payment_msisdn_region_id and t1.channel_id = t2.channel_id and t1.order_user_id = t2.order_user_id)   
 group by t1.sub_business_id
         ,t1.order_msisdn_region_id
         ,t1.payment_msisdn_region_id
         ,t1.channel_id  
),  


tmp_after_4_mon_in_order_user as(  
select t1.sub_business_id
      ,t1.order_msisdn_region_id
      ,t1.payment_msisdn_region_id
      ,t1.channel_id
      ,count(t2.order_user_id) as after_4_mon_retention_user_cnt  --4个月留存用户数 
  from tmp_new_add_order_user t1        
  left join (                           --4个月最后一天在订
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
                      from rptdata.fact_order_daily_snapshot a 
                     where translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),4)), '-', '') >= '20170601'
                       and a.snapshot_day = translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),4)), '-', '')
                     
                     union all
                     
                    select a.sub_busi_id as sub_business_id
                          ,a.region_id as order_msisdn_region_id
                          ,a.region_id as payment_msisdn_region_id
                          ,a.chn_id_new as channel_id
                          ,b.user_id as order_user_id
                      from cdmp_dw.td_aaa_order_d a 
                      join rptdata.dim_userid_usernum b
                        on a.serv_number = b.user_num
                     where translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),4)), '-', '') <= '20170531'
                       and a.src_source_day = translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),4)), '-', '') 
                    ) t
              group by sub_business_id
                      ,order_msisdn_region_id
                      ,payment_msisdn_region_id
                      ,channel_id
                      ,order_user_id    
           ) t2 
    on (t1.sub_business_id = t2.sub_business_id and t1.order_msisdn_region_id = t2.order_msisdn_region_id 
        and t1.payment_msisdn_region_id = t2.payment_msisdn_region_id and t1.channel_id = t2.channel_id and t1.order_user_id = t2.order_user_id)    
 group by t1.sub_business_id
         ,t1.order_msisdn_region_id
         ,t1.payment_msisdn_region_id
         ,t1.channel_id  
),  


tmp_after_5_mon_in_order_user as(  
select t1.sub_business_id
      ,t1.order_msisdn_region_id
      ,t1.payment_msisdn_region_id
      ,t1.channel_id
      ,count(t2.order_user_id) as after_5_mon_retention_user_cnt  --5个月留存用户数 
  from tmp_new_add_order_user t1        
  left join (                           --5个月最后一天在订
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
                      from rptdata.fact_order_daily_snapshot a 
                     where translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),5)), '-', '') >= '20170601'
                       and a.snapshot_day = translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),5)), '-', '')
                     
                     union all
                     
                    select a.sub_busi_id as sub_business_id
                          ,a.region_id as order_msisdn_region_id
                          ,a.region_id as payment_msisdn_region_id
                          ,a.chn_id_new as channel_id
                          ,b.user_id as order_user_id
                      from cdmp_dw.td_aaa_order_d a 
                      join rptdata.dim_userid_usernum b
                        on a.serv_number = b.user_num
                     where translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),5)), '-', '') <= '20170531'
                       and a.src_source_day = translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),5)), '-', '') 
                    ) t
              group by sub_business_id
                      ,order_msisdn_region_id
                      ,payment_msisdn_region_id
                      ,channel_id
                      ,order_user_id    
           ) t2 
    on (t1.sub_business_id = t2.sub_business_id and t1.order_msisdn_region_id = t2.order_msisdn_region_id 
        and t1.payment_msisdn_region_id = t2.payment_msisdn_region_id and t1.channel_id = t2.channel_id and t1.order_user_id = t2.order_user_id)    
 group by t1.sub_business_id
         ,t1.order_msisdn_region_id
         ,t1.payment_msisdn_region_id
         ,t1.channel_id  
), 


tmp_after_6_mon_in_order_user as(  
select t1.sub_business_id
      ,t1.order_msisdn_region_id
      ,t1.payment_msisdn_region_id
      ,t1.channel_id
      ,count(t2.order_user_id) as after_6_mon_retention_user_cnt  --6个月留存用户数 
  from tmp_new_add_order_user t1        
  left join (                           --6个月最后一天在订
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
                      from rptdata.fact_order_daily_snapshot a 
                     where translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),6)), '-', '') >= '20170601'
                       and a.snapshot_day = translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),6)), '-', '')
                     
                     union all
                     
                    select a.sub_busi_id as sub_business_id
                          ,a.region_id as order_msisdn_region_id
                          ,a.region_id as payment_msisdn_region_id
                          ,a.chn_id_new as channel_id
                          ,b.user_id as order_user_id
                      from cdmp_dw.td_aaa_order_d a 
                      join rptdata.dim_userid_usernum b
                        on a.serv_number = b.user_num
                     where translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),6)), '-', '') <= '20170531'
                       and a.src_source_day = translate(last_day(add_months(concat_ws('-', substr('${EXTRACT_MONTH}', 1, 4), substr('${EXTRACT_MONTH}', 5, 2), '01'),6)), '-', '') 
                    ) t
              group by sub_business_id
                      ,order_msisdn_region_id
                      ,payment_msisdn_region_id
                      ,channel_id
                      ,order_user_id    
           ) t2 
    on (t1.sub_business_id = t2.sub_business_id and t1.order_msisdn_region_id = t2.order_msisdn_region_id 
        and t1.payment_msisdn_region_id = t2.payment_msisdn_region_id and t1.channel_id = t2.channel_id and t1.order_user_id = t2.order_user_id)      
 group by t1.sub_business_id
         ,t1.order_msisdn_region_id
         ,t1.payment_msisdn_region_id
         ,t1.channel_id  
) 





select '${EXTRACT_MONTH}' as statis_month                  --新增订购月份
      ,nvl(business_type, '剔重汇总') as business_type
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
        select nvl(h.b_type, 'NA') as business_type                             
              ,nvl(h.business_name, 'NA') as business_name                      
              ,nvl(h.sub_busi_name, 'NA') as sub_business_name                         
              ,nvl(i.prov_name, nvl(j.prov_name, 'NA')) as phone_province_name  
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
          from tmp_new_add_order_user a
          left join rptdata.dim_server h
            on (case when a.sub_business_id = '-998' then concat('', rand()) else a.sub_business_id end) = h.sub_busi_id
          left join rptdata.dim_region i
            on (case when a.order_msisdn_region_id = '-998' then concat('', rand()) else a.order_msisdn_region_id end) = i.region_id
          left join rptdata.dim_region j
            on (case when a.payment_msisdn_region_id = '-998' then concat('', rand()) else a.payment_msisdn_region_id end) = j.region_id  
          left join rptdata.dim_chn g
            on (case when a.channel_id = '-998' then concat('', rand()) else a.channel_id end) = g.chn_id 
         group by nvl(h.b_type, 'NA')     
                 ,nvl(h.business_name, 'NA')                    
                 ,nvl(h.sub_busi_name, 'NA')                        
                 ,nvl(i.prov_name, nvl(j.prov_name, 'NA'))  
                 ,a.channel_id                                                       
                 ,nvl(g.chn_name, 'NA')                           
                 ,nvl(g.chn_attr_1_name, 'NA')               
                 ,nvl(g.chn_attr_2_name, 'NA')
                          
         union all 

        select nvl(h.b_type, 'NA') as business_type                             --业务类型
              ,nvl(h.business_name, 'NA') as business_name                      --业务名称
              ,nvl(h.sub_busi_name, 'NA') as sub_business_name                  --子业务名称       
              ,nvl(i.prov_name, nvl(j.prov_name, 'NA')) as phone_province_name  --省份
              ,a.channel_id                                                     --渠道id      
              ,nvl(g.chn_name, 'NA') as chn_name                                --渠道名称
              ,nvl(g.chn_attr_1_name, 'NA') as chn_attr_1_name                  --渠道一级
              ,nvl(g.chn_attr_2_name, 'NA') as chn_attr_2_name                  --渠道二级
              ,0 as new_add_order_user_cnt
              ,a.this_mon_retention_user_cnt
              ,0 as second_mon_retention_user_cnt
              ,0 as after_2_mon_retention_user_cnt
              ,0 as after_3_mon_retention_user_cnt
              ,0 as after_4_mon_retention_user_cnt
              ,0 as after_5_mon_retention_user_cnt
              ,0 as after_6_mon_retention_user_cnt 
          from tmp_this_mon_in_order_user a
          left join rptdata.dim_server h
            on (case when a.sub_business_id = '-998' then concat('', rand()) else a.sub_business_id end) = h.sub_busi_id
          left join rptdata.dim_region i
            on (case when a.order_msisdn_region_id = '-998' then concat('', rand()) else a.order_msisdn_region_id end) = i.region_id
          left join rptdata.dim_region j
            on (case when a.payment_msisdn_region_id = '-998' then concat('', rand()) else a.payment_msisdn_region_id end) = j.region_id  
          left join rptdata.dim_chn g
            on (case when a.channel_id = '-998' then concat('', rand()) else a.channel_id end) = g.chn_id 
          
         union all 

        select nvl(h.b_type, 'NA') as business_type                             --业务类型
              ,nvl(h.business_name, 'NA') as business_name                      --业务名称
              ,nvl(h.sub_busi_name, 'NA') as sub_business_name                  --子业务名称       
              ,nvl(i.prov_name, nvl(j.prov_name, 'NA')) as phone_province_name  --省份
              ,a.channel_id                                                     --渠道id      
              ,nvl(g.chn_name, 'NA') as chn_name                                --渠道名称
              ,nvl(g.chn_attr_1_name, 'NA') as chn_attr_1_name                  --渠道一级
              ,nvl(g.chn_attr_2_name, 'NA') as chn_attr_2_name                  --渠道二级
              ,0 as new_add_order_user_cnt
              ,0 as this_mon_retention_user_cnt
              ,a.second_mon_retention_user_cnt
              ,0 as after_2_mon_retention_user_cnt
              ,0 as after_3_mon_retention_user_cnt
              ,0 as after_4_mon_retention_user_cnt
              ,0 as after_5_mon_retention_user_cnt
              ,0 as after_6_mon_retention_user_cnt 
          from tmp_second_mon_in_order_user a
          left join rptdata.dim_server h
            on (case when a.sub_business_id = '-998' then concat('', rand()) else a.sub_business_id end) = h.sub_busi_id
          left join rptdata.dim_region i
            on (case when a.order_msisdn_region_id = '-998' then concat('', rand()) else a.order_msisdn_region_id end) = i.region_id
          left join rptdata.dim_region j
            on (case when a.payment_msisdn_region_id = '-998' then concat('', rand()) else a.payment_msisdn_region_id end) = j.region_id  
          left join rptdata.dim_chn g
            on (case when a.channel_id = '-998' then concat('', rand()) else a.channel_id end) = g.chn_id 

         union all 

        select nvl(h.b_type, 'NA') as business_type                             --业务类型
              ,nvl(h.business_name, 'NA') as business_name                      --业务名称
              ,nvl(h.sub_busi_name, 'NA') as sub_business_name                  --子业务名称       
              ,nvl(i.prov_name, nvl(j.prov_name, 'NA')) as phone_province_name  --省份
              ,a.channel_id                                                     --渠道id      
              ,nvl(g.chn_name, 'NA') as chn_name                                --渠道名称
              ,nvl(g.chn_attr_1_name, 'NA') as chn_attr_1_name                  --渠道一级
              ,nvl(g.chn_attr_2_name, 'NA') as chn_attr_2_name                  --渠道二级
              ,0 as new_add_order_user_cnt
              ,0 as this_mon_retention_user_cnt
              ,0 as second_mon_retention_user_cnt
              ,a.after_2_mon_retention_user_cnt
              ,0 as after_3_mon_retention_user_cnt
              ,0 as after_4_mon_retention_user_cnt
              ,0 as after_5_mon_retention_user_cnt
              ,0 as after_6_mon_retention_user_cnt 
          from tmp_after_2_mon_in_order_user a
          left join rptdata.dim_server h
            on (case when a.sub_business_id = '-998' then concat('', rand()) else a.sub_business_id end) = h.sub_busi_id
          left join rptdata.dim_region i
            on (case when a.order_msisdn_region_id = '-998' then concat('', rand()) else a.order_msisdn_region_id end) = i.region_id
          left join rptdata.dim_region j
            on (case when a.payment_msisdn_region_id = '-998' then concat('', rand()) else a.payment_msisdn_region_id end) = j.region_id  
          left join rptdata.dim_chn g
            on (case when a.channel_id = '-998' then concat('', rand()) else a.channel_id end) = g.chn_id 

         union all 

        select nvl(h.b_type, 'NA') as business_type                             --业务类型
              ,nvl(h.business_name, 'NA') as business_name                      --业务名称
              ,nvl(h.sub_busi_name, 'NA') as sub_business_name                  --子业务名称       
              ,nvl(i.prov_name, nvl(j.prov_name, 'NA')) as phone_province_name  --省份
              ,a.channel_id                                                     --渠道id      
              ,nvl(g.chn_name, 'NA') as chn_name                                --渠道名称
              ,nvl(g.chn_attr_1_name, 'NA') as chn_attr_1_name                  --渠道一级
              ,nvl(g.chn_attr_2_name, 'NA') as chn_attr_2_name                  --渠道二级
              ,0 as new_add_order_user_cnt
              ,0 as this_mon_retention_user_cnt
              ,0 as second_mon_retention_user_cnt
              ,0 as after_2_mon_retention_user_cnt
              ,a.after_3_mon_retention_user_cnt
              ,0 as after_4_mon_retention_user_cnt
              ,0 as after_5_mon_retention_user_cnt
              ,0 as after_6_mon_retention_user_cnt 
          from tmp_after_3_mon_in_order_user a
          left join rptdata.dim_server h
            on (case when a.sub_business_id = '-998' then concat('', rand()) else a.sub_business_id end) = h.sub_busi_id
          left join rptdata.dim_region i
            on (case when a.order_msisdn_region_id = '-998' then concat('', rand()) else a.order_msisdn_region_id end) = i.region_id
          left join rptdata.dim_region j
            on (case when a.payment_msisdn_region_id = '-998' then concat('', rand()) else a.payment_msisdn_region_id end) = j.region_id  
          left join rptdata.dim_chn g
            on (case when a.channel_id = '-998' then concat('', rand()) else a.channel_id end) = g.chn_id 

         union all 

        select nvl(h.b_type, 'NA') as business_type                             --业务类型
              ,nvl(h.business_name, 'NA') as business_name                      --业务名称
              ,nvl(h.sub_busi_name, 'NA') as sub_business_name                  --子业务名称       
              ,nvl(i.prov_name, nvl(j.prov_name, 'NA')) as phone_province_name  --省份
              ,a.channel_id                                                     --渠道id      
              ,nvl(g.chn_name, 'NA') as chn_name                                --渠道名称
              ,nvl(g.chn_attr_1_name, 'NA') as chn_attr_1_name                  --渠道一级
              ,nvl(g.chn_attr_2_name, 'NA') as chn_attr_2_name                  --渠道二级
              ,0 as new_add_order_user_cnt
              ,0 as this_mon_retention_user_cnt
              ,0 as second_mon_retention_user_cnt
              ,0 as after_2_mon_retention_user_cnt
              ,0 as after_3_mon_retention_user_cnt
              ,a.after_4_mon_retention_user_cnt
              ,0 as after_5_mon_retention_user_cnt
              ,0 as after_6_mon_retention_user_cnt 
          from tmp_after_4_mon_in_order_user a
          left join rptdata.dim_server h
            on (case when a.sub_business_id = '-998' then concat('', rand()) else a.sub_business_id end) = h.sub_busi_id
          left join rptdata.dim_region i
            on (case when a.order_msisdn_region_id = '-998' then concat('', rand()) else a.order_msisdn_region_id end) = i.region_id
          left join rptdata.dim_region j
            on (case when a.payment_msisdn_region_id = '-998' then concat('', rand()) else a.payment_msisdn_region_id end) = j.region_id  
          left join rptdata.dim_chn g
            on (case when a.channel_id = '-998' then concat('', rand()) else a.channel_id end) = g.chn_id 

         union all 

        select nvl(h.b_type, 'NA') as business_type                             --业务类型
              ,nvl(h.business_name, 'NA') as business_name                      --业务名称
              ,nvl(h.sub_busi_name, 'NA') as sub_business_name                  --子业务名称       
              ,nvl(i.prov_name, nvl(j.prov_name, 'NA')) as phone_province_name  --省份
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
              ,a.after_5_mon_retention_user_cnt
              ,0 as after_6_mon_retention_user_cnt 
          from tmp_after_5_mon_in_order_user a
          left join rptdata.dim_server h
            on (case when a.sub_business_id = '-998' then concat('', rand()) else a.sub_business_id end) = h.sub_busi_id
          left join rptdata.dim_region i
            on (case when a.order_msisdn_region_id = '-998' then concat('', rand()) else a.order_msisdn_region_id end) = i.region_id
          left join rptdata.dim_region j
            on (case when a.payment_msisdn_region_id = '-998' then concat('', rand()) else a.payment_msisdn_region_id end) = j.region_id  
          left join rptdata.dim_chn g
            on (case when a.channel_id = '-998' then concat('', rand()) else a.channel_id end) = g.chn_id 

         union all 

        select nvl(h.b_type, 'NA') as business_type                             --业务类型
              ,nvl(h.business_name, 'NA') as business_name                      --业务名称
              ,nvl(h.sub_busi_name, 'NA') as sub_business_name                  --子业务名称       
              ,nvl(i.prov_name, nvl(j.prov_name, 'NA')) as phone_province_name  --省份
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
              ,a.after_6_mon_retention_user_cnt 
          from tmp_after_6_mon_in_order_user a
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
  

