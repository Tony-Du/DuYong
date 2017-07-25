

新增订购用户数 

统计周期内，成功订购业务的用户数。
维度：部门、业务、终端、省份、地市、渠道、付费渠道、货币类型、是否虚拟局数据、订购周期、订购周期单位、是否自动续订   

count(distinct order_user_id) where src_file_day between xxx and order_status in (5,9)  

rptdata.fact_order_item_detail



在订用户数   

统计日23点59分，还存在订购关系，能正常使用业务的用户数，
维度：部门、业务、终端、省份、地市、渠道、付费渠道、货币类型、是否虚拟局数据、订购周期、订购周期单位、是否自动续订   

count(distinct order_user_id) where snapshot_day = xxx  

rptdata.fact_order_daily_snapshot

-----------------------------------

维度：
新增订购月份  业务类型    业务名称    子业务名称   省份  渠道id    渠道名称    渠道一级    渠道二级



select '${THIS_MONTH}' as statis_month                  --新增订购月份
      ,business_type
      ,business_name
      ,sub_business_name
      ,phone_province_name
      ,channel_id
      ,chn_name
      ,chn_attr_1_name
      ,chn_attr_2_name
      ,count(distinct add_order_user_id) as add_order_user_cnt     --新增订购用户数
      ,count(distinct cancel_order_user_id) as cancel_order_user_cnt  --退订用户数      
      ,count(distinct this_mon_retention_user_id) as this_mon_retention_user_cnt       --当月留存用户数
      ,count(distinct second_mon_retention_user_id) as second_mon_retention_user_cnt   --次月留存用户数  
      ,count(distinct after_2_mon_retention_user_id) as after_2_mon_retention_user_cnt --2个月留存用户数    
      ,count(distinct after_3_mon_retention_user_id) as after_3_mon_retention_user_cnt --3个月留存用户数    
      ,count(distinct after_4_mon_retention_user_id) as after_4_mon_retention_user_cnt --4个月留存用户数    
      ,count(distinct after_5_mon_retention_user_id) as after_5_mon_retention_user_cnt --5个月留存用户数    
      ,count(distinct after_6_mon_retention_user_id) as after_6_mon_retention_user_cnt --6个月留存用户数               
      ,grouping__id as grain_ind
  from (
        select nvl(h.b_type, 'NA') as business_type                             --业务类型
              --,nvl(h.business_id, 'NA') as business_id                
              ,nvl(h.business_name, 'NA') as business_name                      --业务名称
              ,nvl(h.sub_busi_name, 'NA') as sub_business_name                  --子业务名称    
              ,nvl(i.prov_name, nvl(j.prov_name, 'NA')) as phone_province_name  --省份
              --,nvl(i.region_name, nvl(j.region_name, 'NA')) as phone_city_name      
              ,a.channel_id                                                     --渠道id
              ,nvl(g.chn_name, 'NA') as chn_name                                --渠道名称
              ,nvl(g.chn_attr_1_name, 'NA') as chn_attr_1_name                  --渠道一级
              ,nvl(g.chn_attr_2_name, 'NA') as chn_attr_2_name                  --渠道二级
              ,case when a.order_status in (5,9) then a.order_user_id else null end as add_order_user_id  --新增订购用户
              ,case when a.order_status in (12,14,15,16) then a.order_user_id else null end as cancel_order_user_id --退订用户              
              ,null as this_mon_retention_user_id
              ,null as second_mon_retention_user_id
              ,null as after_2_mon_retention_user_id
              ,null as after_3_mon_retention_user_id
              ,null as after_4_mon_retention_user_id
              ,null as after_5_mon_retention_user_id
              ,null as after_6_mon_retention_user_id              
          from rptdata.fact_order_item_detail a  
          left join rptdata.dim_server h
            on (case when a.sub_business_id = '-998' then concat('', rand()) else a.sub_business_id end) = h.sub_busi_id
          left join rptdata.dim_region i
            on (case when a.order_msisdn_region_id = '-998' then concat('', rand()) else a.order_msisdn_region_id end) = i.region_id
          left join rptdata.dim_region j
            on (case when a.payment_msisdn_region_id = '-998' then concat('', rand()) else a.payment_msisdn_region_id end) = j.region_id  
          left join rptdata.dim_chn g
            on (case when a.channel_id = '-998' then concat('', rand()) else a.channel_id end) = g.chn_id 
         where substr(a.src_file_day, 1, 6) = '${THIS_MONTH}'
           
         union all

        select nvl(h.b_type, 'NA') as business_type                             --业务类型
              --,nvl(h.business_id, 'NA') as business_id                
              ,nvl(h.business_name, 'NA') as business_name                      --业务名称
              ,nvl(h.sub_busi_name, 'NA') as sub_business_name                  --子业务名称    
              ,nvl(i.prov_name, nvl(j.prov_name, 'NA')) as phone_province_name  --省份
              --,nvl(i.region_name, nvl(j.region_name, 'NA')) as phone_city_name      
              ,a.channel_id                                                     --渠道id
              ,nvl(g.chn_name, 'NA') as chn_name                                --渠道名称
              ,nvl(g.chn_attr_1_name, 'NA') as chn_attr_1_name                  --渠道一级
              ,nvl(g.chn_attr_2_name, 'NA') as chn_attr_2_name                  --渠道二级              
              ,null as add_order_user_id
              ,null as cancel_order_user_id              
              ,case when substr(a.snapshot_day, 1, 6) = '${THIS_MONTH}' 
                    then a.order_user_id else null end as this_mon_retention_user_id   --当月留存用户              
              ,case when substr(a.snapshot_day, 1, 6) = substr(translate(add_months(concat_ws('-', substr('${THIS_MONTH}',1,4), substr('${THIS_MONTH}',5,2), '01'),1),'-', ''), 1, 6)
                    then a.order_user_id else null end as second_mon_retention_user_id --次月留存用户                    
              ,case when substr(a.snapshot_day, 1, 6) = substr(translate(add_months(concat_ws('-', substr('${THIS_MONTH}',1,4), substr('${THIS_MONTH}',5,2), '01'),2),'-', ''), 1, 6)
                    then a.order_user_id else null end as after_2_mon_retention_user_id --2个月留存用户                    
              ,case when substr(a.snapshot_day, 1, 6) = substr(translate(add_months(concat_ws('-', substr('${THIS_MONTH}',1,4), substr('${THIS_MONTH}',5,2), '01'),3),'-', ''), 1, 6)
                    then a.order_user_id else null end as after_3_mon_retention_user_id --3个月留存用户                    
              ,case when substr(a.snapshot_day, 1, 6) = substr(translate(add_months(concat_ws('-', substr('${THIS_MONTH}',1,4), substr('${THIS_MONTH}',5,2), '01'),4),'-', ''), 1, 6)
                    then a.order_user_id else null end as after_4_mon_retention_user_id --4个月留存用户                    
              ,case when substr(a.snapshot_day, 1, 6) = substr(translate(add_months(concat_ws('-', substr('${THIS_MONTH}',1,4), substr('${THIS_MONTH}',5,2), '01'),5),'-', ''), 1, 6)
                    then a.order_user_id else null end as after_5_mon_retention_user_id --5个月留存用户                    
              ,case when substr(a.snapshot_day, 1, 6) = substr(translate(add_months(concat_ws('-', substr('${THIS_MONTH}',1,4), substr('${THIS_MONTH}',5,2), '01'),6),'-', ''), 1, 6)
                    then a.order_user_id else null end as after_6_mon_retention_user_id --6个月留存用户              
          from rptdata.fact_order_daily_snapshot a  
          left join rptdata.dim_server h
            on (case when a.sub_business_id = '-998' then concat('', rand()) else a.sub_business_id end) = h.sub_busi_id
          left join rptdata.dim_region i
            on (case when a.order_msisdn_region_id = '-998' then concat('', rand()) else a.order_msisdn_region_id end) = i.region_id
          left join rptdata.dim_region j
            on (case when a.payment_msisdn_region_id = '-998' then concat('', rand()) else a.payment_msisdn_region_id end) = j.region_id  
          left join rptdata.dim_chn g
            on (case when a.channel_id = '-998' then concat('', rand()) else a.channel_id end) = g.chn_id    
         where substr(a.snapshot_day, 1, 6) between '${THIS_MONTH}' 
                                            and substr(translate(add_months(concat_ws('-', substr('${THIS_MONTH}',1,4), substr('${THIS_MONTH}',5,2), '01'),6),'-', ''), 1, 6)            
       ) t     
 group by business_type,business_name,sub_business_name,channel_id
         ,chn_name,chn_attr_1_name,chn_attr_2_name,phone_province_name   
grouping sets (
(business_type, business_name, sub_business_name, channel_id, chn_name, chn_attr_1_name, chn_attr_2_name),
(business_type, business_name, sub_business_name, channel_id, chn_name, chn_attr_1_name, chn_attr_2_name, phone_province_name)
);   
   
   


   


   
--from rptdata.fact_order_daily_snapshot a  
--left join rptdata.dim_server h
--on (case when a.sub_business_id = '-998' then concat('', rand()) else a.sub_business_id end) = h.sub_busi_id
假设表 a 的 sub_business_id 的值为‘-998’的有9000w条，为正常值的只有1条，
如果不加任何处理，那么join的时候，为‘-998’的数据将会集中在一个分区(节点)处理，数据倾斜
如果为其附上一个 rand() 随机数，为‘-998’的数据将会被分散到不同分区(节点)处理，hive(MR)优化  
spark 中不能用   
   
   