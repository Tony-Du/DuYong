

select substr('20170601', 1, 6) as statis_month                  --新增订购月份
      ,business_type
      ,business_name
      ,sub_business_name
      ,phone_province_name
      ,channel_id
      ,chn_name
      ,chn_attr_1_name
      ,chn_attr_2_name
      ,count(distinct order_user_id) as add_order_user_cnt     --新增订购用户数
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
              ,a.order_user_id                                                  --新增订购用户
          from rptdata.fact_order_item_detail a  
          left join rptdata.dim_server h
            on (case when a.sub_business_id = '-998' then concat('', rand()) else a.sub_business_id end) = h.sub_busi_id
          left join rptdata.dim_region i
            on (case when a.order_msisdn_region_id = '-998' then concat('', rand()) else a.order_msisdn_region_id end) = i.region_id
          left join rptdata.dim_region j
            on (case when a.payment_msisdn_region_id = '-998' then concat('', rand()) else a.payment_msisdn_region_id end) = j.region_id  
          left join rptdata.dim_chn g
            on (case when a.channel_id = '-998' then concat('', rand()) else a.channel_id end) = g.chn_id    
         where a.order_status in (5,9)
           and a.src_file_day between '20170601' and '20170601'
       ) t     
 group by business_type,business_name,sub_business_name,channel_id
         ,chn_name,chn_attr_1_name,chn_attr_2_name,phone_province_name   
grouping sets (
(business_type, business_name, sub_business_name, channel_id, chn_name, chn_attr_1_name, chn_attr_2_name),
(business_type, business_name, sub_business_name, channel_id, chn_name, chn_attr_1_name, chn_attr_2_name, phone_province_name)
); 



+---------------+----------------+----------------+--------------------+----------------------+------------------+----------------------------------------------------+------------------+-----------------------+---------------------+--+
| statis_month  | business_type  | business_name  | sub_business_name  | phone_province_name  |    channel_id    |                      chn_name                      | chn_attr_1_name  |    chn_attr_2_name    | add_order_user_cnt  |
+---------------+----------------+----------------+--------------------+----------------------+------------------+----------------------------------------------------+------------------+-----------------------+---------------------+--+
| 201706        | 品牌             | 第一视频手机视频       | 第一视频手机视频           | 重庆                   | 10063302100      | 省BOSS开通                                            | 移动自有渠道           | 省公司                   | 1                   |
| 201706        | 品牌             | 芒果TV           | 芒果TV               | NULL                 | 10063302100      | 省BOSS开通                                            | 移动自有渠道           | 省公司                   | 3203                |
| 201706        | 品牌             | 芒果TV           | 芒果TV               | 上海                   | 10063302100      | 省BOSS开通                                            | 移动自有渠道           | 省公司                   | 128                 |
| 201706        | 品牌             | 芒果TV           | 芒果TV               | 云南                   | 10063302100      | 省BOSS开通                                            | 移动自有渠道           | 省公司                   | 94                  |
| 201706        | 品牌             | 芒果TV           | 芒果TV               | 北京                   | 10063302100      | 省BOSS开通                                            | 移动自有渠道           | 省公司                   | 72                  |
| 201706        | 品牌             | 芒果TV           | 芒果TV               | 四川                   | 10063302100      | 省BOSS开通                                            | 移动自有渠道           | 省公司                   | 59                  |
| 201706        | 品牌             | 芒果TV           | 芒果TV               | 安徽                   | 10063302100      | 省BOSS开通                                            | 移动自有渠道           | 省公司                   | 24                  |
| 201706        | 品牌             | 芒果TV           | 芒果TV               | 江苏                   | 10063302100      | 省BOSS开通                                            | 移动自有渠道           | 省公司                   | 204                 |
| 201706        | 品牌             | 芒果TV           | 芒果TV               | 江西                   | 10063302100      | 省BOSS开通                                            | 移动自有渠道           | 省公司                   | 10                  |
| 201706        | 品牌             | 芒果TV           | 芒果TV               | 河南                   | 10063302100      | 省BOSS开通                                            | 移动自有渠道           | 省公司                   | 135                 |
| 201706        | 品牌             | 芒果TV           | 芒果TV               | 浙江                   | 10063302100      | 省BOSS开通                                            | 移动自有渠道           | 省公司                   | 162                 |
| 201706        | 品牌             | 芒果TV           | 芒果TV               | 湖南                   | 10063302100      | 省BOSS开通                                            | 移动自有渠道           | 省公司                   | 28                  |
| 201706        | 品牌             | 芒果TV           | 芒果TV               | 甘肃                   | 10063302100      | 省BOSS开通                                            | 移动自有渠道           | 省公司                   | 139                 |
| 201706        | 品牌             | 芒果TV           | 芒果TV               | 福建                   | 10063302100      | 省BOSS开通                                            | 移动自有渠道           | 省公司                   | 32                  |
| 201706        | 品牌             | 芒果TV           | 芒果TV               | 西藏                   | 10063302100      | 省BOSS开通                                            | 移动自有渠道           | 省公司                   | 40                  |
| 201706        | 品牌             | 芒果TV           | 芒果TV               | 重庆                   | 10063302100      | 省BOSS开通                                            | 移动自有渠道           | 省公司                   | 112                 |
| 201706        | 品牌             | 芒果TV           | 芒果TV               | 青海                   | 10063302100      | 省BOSS开通                                            | 移动自有渠道           | 省公司                   | 23                  |
| 201706        | 品牌             | 芒果TV           | 芒果TV               | NA                   | 302800310000000  | 芒果-TV移动版                                           | 品牌栏目渠道           | 芒果TV                  | 1                   |
| 201706        | 品牌             | 芒果TV           | 芒果TV               | 内蒙古                  | 307300000000011  | 合作伙伴渠道_品牌-芒果TV-芒果TV_SDK推广_杭州哲信信息技术有限公司_001         | 合作伙伴渠道           | 品牌-芒果TV-芒果TV          | 14                  |
| 201706        | 品牌             | 芒果TV           | 芒果TV               | 吉林                   | 307300000000011  | 合作伙伴渠道_品牌-芒果TV-芒果TV_SDK推广_杭州哲信信息技术有限公司_001         | 合作伙伴渠道           | 品牌-芒果TV-芒果TV          | 10                  |
| 201706        | 品牌             | 芒果TV           | 芒果TV               | 天津                   | 307300000000011  | 合作伙伴渠道_品牌-芒果TV-芒果TV_SDK推广_杭州哲信信息技术有限公司_001         | 合作伙伴渠道           | 品牌-芒果TV-芒果TV          | 15                  |
| 201706        | 品牌             | 芒果TV           | 芒果TV               | 湖北                   | 307300000000011  | 合作伙伴渠道_品牌-芒果TV-芒果TV_SDK推广_杭州哲信信息技术有限公司_001         | 合作伙伴渠道           | 品牌-芒果TV-芒果TV          | 656                 |
| 201706        | 品牌             | 芒果TV           | 芒果TV               | 辽宁                   | 307300000000011  | 合作伙伴渠道_品牌-芒果TV-芒果TV_SDK推广_杭州哲信信息技术有限公司_001         | 合作伙伴渠道           | 品牌-芒果TV-芒果TV          | 156                 |



select substr(translate(add_months(concat_ws('-', substr('201706',1,4), substr('201706',5,2), '01'),1),'-', ''), 1, 6) 





select '201612' as statis_month                  --新增订购月份
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
         where substr(a.src_file_day, 1, 6) = '201612'
           
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
              ,case when substr(a.snapshot_day, 1, 6) = '201612' 
                    then a.order_user_id else null end as this_mon_retention_user_id   --当月留存用户              
              ,case when substr(a.snapshot_day, 1, 6) = substr(translate(add_months(concat_ws('-', substr('201612',1,4), substr('201612',5,2), '01'),1),'-', ''), 1, 6)
                    then a.order_user_id else null end as second_mon_retention_user_id --次月留存用户                    
              ,case when substr(a.snapshot_day, 1, 6) = substr(translate(add_months(concat_ws('-', substr('201612',1,4), substr('201612',5,2), '01'),2),'-', ''), 1, 6)
                    then a.order_user_id else null end as after_2_mon_retention_user_id --2个月留存用户                    
              ,case when substr(a.snapshot_day, 1, 6) = substr(translate(add_months(concat_ws('-', substr('201612',1,4), substr('201612',5,2), '01'),3),'-', ''), 1, 6)
                    then a.order_user_id else null end as after_3_mon_retention_user_id --3个月留存用户                    
              ,case when substr(a.snapshot_day, 1, 6) = substr(translate(add_months(concat_ws('-', substr('201612',1,4), substr('201612',5,2), '01'),4),'-', ''), 1, 6)
                    then a.order_user_id else null end as after_4_mon_retention_user_id --4个月留存用户                    
              ,case when substr(a.snapshot_day, 1, 6) = substr(translate(add_months(concat_ws('-', substr('201612',1,4), substr('201612',5,2), '01'),5),'-', ''), 1, 6)
                    then a.order_user_id else null end as after_5_mon_retention_user_id --5个月留存用户                    
              ,case when substr(a.snapshot_day, 1, 6) = substr(translate(add_months(concat_ws('-', substr('201612',1,4), substr('201612',5,2), '01'),6),'-', ''), 1, 6)
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
         where substr(a.snapshot_day, 1, 6) between '201612' 
                                            and substr(translate(add_months(concat_ws('-', substr('201612',1,4), substr('201612',5,2), '01'),6),'-', ''), 1, 6)            
       ) t     
 group by business_type,business_name,sub_business_name,channel_id
         ,chn_name,chn_attr_1_name,chn_attr_2_name,phone_province_name   
grouping sets (
(business_type, business_name, sub_business_name, channel_id, chn_name, chn_attr_1_name, chn_attr_2_name),
(business_type, business_name, sub_business_name, channel_id, chn_name, chn_attr_1_name, chn_attr_2_name, phone_province_name)
);  