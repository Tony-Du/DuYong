--使用 left join 那一种思路的时候， 201701 导入到 oracle的数据 是 5292 （去掉 剔重汇总）条

-- 使用第二种思路，即下面这种 union all 的思路， 最后查出来的数据也是 5292 条

--新增用户 union all 在订用户
--然后sum()
--最后去除 新增用户数为0 的记录





with tmp_new_add_order_user as(                       --当前周期(如：456份，三个月)新增
 select nvl(h.b_type, 'NA') as business_type                             --业务类型
              ,nvl(h.business_name, 'NA') as business_name                      --业务名称
              ,nvl(h.sub_busi_name, 'NA') as sub_business_name                  --子业务名称       
              ,nvl(i.prov_name, nvl(j.prov_name, 'NA')) as phone_province_name  --省份
              ,a.channel_id                                                     --渠道id      
              ,nvl(g.chn_name, 'NA') as chn_name                                --渠道名称
              ,nvl(g.chn_attr_1_name, 'NA') as chn_attr_1_name                  --渠道一级
              ,nvl(g.chn_attr_2_name, 'NA') as chn_attr_2_name                  --渠道二级
              ,a.order_user_id
          from rptdata.fact_order_item_detail a 
          left join rptdata.dim_server h
            on (case when a.sub_business_id = '-998' then concat('', rand()) else a.sub_business_id end) = h.sub_busi_id
          left join rptdata.dim_region i
            on (case when a.order_msisdn_region_id = '-998' then concat('', rand()) else a.order_msisdn_region_id end) = i.region_id
          left join rptdata.dim_region j
            on (case when a.payment_msisdn_region_id = '-998' then concat('', rand()) else a.payment_msisdn_region_id end) = j.region_id  
          left join rptdata.dim_chn g
            on (case when a.channel_id = '-998' then concat('', rand()) else a.channel_id end) = g.chn_id           
         where a.src_file_day >= '20170601' 
           and a.src_file_day between '20170101' and '20170131' 
           and a.order_status in (5,9)  
           
         union all
         
        select nvl(h.b_type, 'NA') as business_type                             --业务类型
              ,nvl(h.business_name, 'NA') as business_name                      --业务名称
              ,nvl(h.sub_busi_name, 'NA') as sub_business_name                  --子业务名称       
              ,nvl(i.prov_name, nvl(j.prov_name, 'NA')) as phone_province_name  --省份
              ,a.chn_id_new as channel_id                                       --渠道id      
              ,nvl(g.chn_name, 'NA') as chn_name                                --渠道名称
              ,nvl(g.chn_attr_1_name, 'NA') as chn_attr_1_name                  --渠道一级
              ,nvl(g.chn_attr_2_name, 'NA') as chn_attr_2_name                  --渠道二级
              ,b.user_id as order_user_id
          from cdmp_dw.td_aaa_order_log_d a 
          join rptdata.dim_userid_usernum b
            on a.serv_number = b.user_num
          left join rptdata.dim_server h
            on (case when a.sub_busi_id = '-998' then concat('', rand()) else a.sub_busi_id end) = h.sub_busi_id
          left join rptdata.dim_region i
            on (case when a.region_id = '-998' then concat('', rand()) else a.region_id end) = i.region_id
          left join rptdata.dim_region j
            on (case when a.region_id = '-998' then concat('', rand()) else a.region_id end) = j.region_id  
          left join rptdata.dim_chn g
            on (case when a.chn_id_new = '-998' then concat('', rand()) else a.chn_id_new end) = g.chn_id 
         where a.src_source_day <= '20170531'
           and a.src_source_day between '20170101' and '20170131'
           and a.order_type = 1  
),

tmp_one_day_in_order_user as(       --某一天在订
 select nvl(h.b_type, 'NA') as business_type                             --业务类型
       ,nvl(h.business_name, 'NA') as business_name                      --业务名称
       ,nvl(h.sub_busi_name, 'NA') as sub_business_name                  --子业务名称       
       ,nvl(i.prov_name, nvl(j.prov_name, 'NA')) as phone_province_name  --省份
       ,a.channel_id                                                     --渠道id      
       ,nvl(g.chn_name, 'NA') as chn_name                                --渠道名称
       ,nvl(g.chn_attr_1_name, 'NA') as chn_attr_1_name                  --渠道一级
       ,nvl(g.chn_attr_2_name, 'NA') as chn_attr_2_name                  --渠道二级
       ,a.order_user_id                         
   from rptdata.fact_order_daily_snapshot a 
   left join rptdata.dim_server h
     on (case when a.sub_business_id = '-998' then concat('', rand()) else a.sub_business_id end) = h.sub_busi_id
   left join rptdata.dim_region i
     on (case when a.order_msisdn_region_id = '-998' then concat('', rand()) else a.order_msisdn_region_id end) = i.region_id
   left join rptdata.dim_region j
     on (case when a.payment_msisdn_region_id = '-998' then concat('', rand()) else a.payment_msisdn_region_id end) = j.region_id  
   left join rptdata.dim_chn g
     on (case when a.channel_id = '-998' then concat('', rand()) else a.channel_id end) = g.chn_id                        
  where '20170610' >= '20170601'
    and a.snapshot_day = '20170610'  
                              
  union all
  
 select nvl(h.b_type, 'NA') as business_type                             --业务类型
       ,nvl(h.business_name, 'NA') as business_name                      --业务名称
       ,nvl(h.sub_busi_name, 'NA') as sub_business_name                  --子业务名称       
       ,nvl(i.prov_name, nvl(j.prov_name, 'NA')) as phone_province_name  --省份
       ,a.chn_id_new as channel_id                                       --渠道id      
       ,nvl(g.chn_name, 'NA') as chn_name                                --渠道名称
       ,nvl(g.chn_attr_1_name, 'NA') as chn_attr_1_name                  --渠道一级
       ,nvl(g.chn_attr_2_name, 'NA') as chn_attr_2_name                  --渠道二级
       ,b.user_id as order_user_id       
   from cdmp_dw.td_aaa_order_d a  
   join rptdata.dim_userid_usernum b 
     on a.serv_number = b.user_num
   left join rptdata.dim_server h
     on (case when a.sub_busi_id = '-998' then concat('', rand()) else a.sub_busi_id end) = h.sub_busi_id
   left join rptdata.dim_region i
     on (case when a.region_id = '-998' then concat('', rand()) else a.region_id end) = i.region_id
   left join rptdata.dim_region j
     on (case when a.region_id = '-998' then concat('', rand()) else a.region_id end) = j.region_id  
   left join rptdata.dim_chn g
     on (case when a.chn_id_new = '-998' then concat('', rand()) else a.chn_id_new end) = g.chn_id                        
  where '20170610' <= '20170531'
    and a.src_source_day = '20170610'                                  
  
)

select tt.business_type
      ,tt.business_name
      ,tt.sub_business_name
      ,tt.phone_province_name
      ,tt.channel_id
      ,tt.chn_name
      ,tt.chn_attr_1_name
      ,tt.chn_attr_2_name
      ,tt.new_add_order_user_cnt
      ,tt.one_day_in_order_user_cnt as one_day_retention_user_cnt
  from (
        select t.business_type
              ,t.business_name
              ,t.sub_business_name
              ,t.phone_province_name
              ,t.channel_id
              ,t.chn_name
              ,t.chn_attr_1_name
              ,t.chn_attr_2_name
              ,sum(t.new_add_order_user_cnt) as new_add_order_user_cnt
              ,sum(t.one_day_in_order_user_cnt) as one_day_in_order_user_cnt
          from (
                select a.business_type
                      ,a.business_name
                      ,a.sub_business_name
                      ,a.phone_province_name
                      ,a.channel_id
                      ,a.chn_name
                      ,a.chn_attr_1_name
                      ,a.chn_attr_2_name
                      ,count(distinct a.order_user_id) as new_add_order_user_cnt
                      ,0 as one_day_in_order_user_cnt
                  from tmp_new_add_order_user a 
                 group by a.business_type
                         ,a.business_name
                         ,a.sub_business_name
                         ,a.phone_province_name
                         ,a.channel_id
                         ,a.chn_name
                         ,a.chn_attr_1_name
                         ,a.chn_attr_2_name
                         
               --  union all
               --  
               -- select d1.business_type
               --       ,d1.business_name
               --       ,d1.sub_business_name
               --       ,d1.phone_province_name
               --       ,d1.channel_id
               --       ,d1.chn_name
               --       ,d1.chn_attr_1_name
               --       ,d1.chn_attr_2_name
               --       ,0 as new_add_order_user_cnt
               --       ,count(distinct d1.order_user_id) as one_day_in_order_user_cnt
               --   from tmp_one_day_in_order_user d1
               --  group by d1.business_type
               --          ,d1.business_name
               --          ,d1.sub_business_name
               --          ,d1.phone_province_name
               --          ,d1.channel_id
               --          ,d1.chn_name
               --          ,d1.chn_attr_1_name
               --          ,d1.chn_attr_2_name
               ) t
         group by t.business_type
                 ,t.business_name
                 ,t.sub_business_name
                 ,t.phone_province_name
                 ,t.channel_id
                 ,t.chn_name
                 ,t.chn_attr_1_name
                 ,t.chn_attr_2_name
       ) tt  
 where tt.new_add_order_user_cnt <> 0;   
 
 剔重汇总