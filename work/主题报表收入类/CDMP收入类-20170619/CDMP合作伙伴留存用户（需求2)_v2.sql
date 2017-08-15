
with tmp_new_add_order_user as(                       --当前周期(如：456份，三个月)新增

select a.order_msisdn_region_id                       --用来关联省份
      ,a.payment_msisdn_region_id      
      
      ,d1.phone_num as order_phone_num                --手机号码  可不导入     
      ,d2.channel_id                                  --渠道ID    可不导入
      ,d3.business_id                                 --业务ID    可不导入
      ,d4.sub_business_id                             --子业务ID  可不导入     
      
      ,a.product_id                                   --产品ID
      ,a.order_crt_time                               --订购日期
      ,a.order_program_id                             --节目ID
      ,a.order_opr_user_id                            --操作员
      ,a.order_user_id      
  from rptdata.fact_order_item_detail a 
  left join rptdata.dim_server h
    on (case when a.sub_business_id = '-998' then concat('', rand()) else a.sub_business_id end) = h.sub_busi_id  
    
  --相关维度的限定
  join (select phone_num from ${PHONE_NUMBER} group by phone_num) d1 on a.order_phone_num = d1.phone_num
  join (select channel_id from ${CHANNEL_ID} group by channel_id) d2 on a.channel_id = d2.channel_id
  join (select business_id from ${BUSINESS_ID} group by business_id) d3 on h.business_id = d3.business_id
  join (select sub_business_id from ${SUB_BUSINESS_ID} group by sub_business_id) d4 on a.sub_business_id = d4.sub_business_id
 
 where a.src_file_day >= '20170601'   
   and a.src_file_day between '${EXTRACT_START_DAY}' and '${EXTRACT_END_DAY}'  
   and a.order_status in (5,9) 
   
   
      
 union all 

select a.region_id as order_msisdn_region_id
      ,a.region_id as payment_msisdn_region_id  
      
      ,d1.phone_num as order_phone_num     
      ,d2.channel_id 
      ,d3.business_id
      ,d4.sub_business_id      
      
      ,a.new_product_id as product_id      
      ,null as order_crt_time          --在源表中没有
      ,a.program_id as order_program_id
      ,null as order_opr_user_id       --在源表中没有
      ,b.user_id as order_user_id      
  from cdmp_dw.td_aaa_order_log_d a 
  join rptdata.dim_userid_usernum b
    on a.serv_number = b.user_num 

  --相关维度的限定
  join (select phone_num from ${PHONE_NUMBER} group by phone_num) d1 on a.serv_number = d1.phone_num
  join (select channel_id from ${CHANNEL_ID} group by channel_id) d2 on a.chn_id_new = d2.channel_id
  join (select business_id from ${BUSINESS_ID} group by business_id) d3 on a.business_id = d3.business_id
  join (select sub_business_id from ${SUB_BUSINESS_ID} group by sub_business_id) d4 on a.sub_busi_id = d4.sub_business_id    
       
 where a.src_source_day <= '20170531'
   and a.src_source_day between '${EXTRACT_START_DAY}' and '${EXTRACT_END_DAY}'
   and a.order_type = 1   
   
),

tmp_one_day_in_order_user as(  --某一天在订
select a.order_msisdn_region_id
      ,a.payment_msisdn_region_id     
      
      ,a.order_phone_num
      ,a.channel_id
      ,h.business_id
      ,a.sub_business_id     
      
      ,a.product_id
      ,a.order_crt_time
      ,a.order_program_id
      ,a.order_opr_user_id
      ,a.order_user_id      
  from rptdata.fact_order_daily_snapshot a 
  left join rptdata.dim_server h
    on (case when a.sub_business_id = '-998' then concat('', rand()) else a.sub_business_id end) = h.sub_busi_id   
 where '${RETENTION_DAY}' >= '20170601'
   and a.snapshot_day = '${RETENTION_DAY}'
 
 union all
 
select a.region_id as order_msisdn_region_id
      ,a.region_id as payment_msisdn_region_id   
      
      ,a.serv_number as order_phone_num
      ,a.chn_id_new as channel_id
      ,a.business_id
      ,a.sub_busi_id as sub_business_id
      
      ,a.new_product_id as product_id
      ,null as order_crt_time          --在源表中没有
      ,a.program_id as order_program_id
      ,null as order_opr_user_id       --在源表中没有
      ,b.user_id as order_user_id       
  from cdmp_dw.td_aaa_order_d a 
  join rptdata.dim_userid_usernum b
    on a.serv_number = b.user_num
 where '${RETENTION_DAY}' <= '20170531'
   and a.src_source_day = '${RETENTION_DAY}' 
)


select  substr('${EXTRACT_START_DAY}',1,6) as statis_start_month                  --新增订购周期的第一个月份
       ,substr('${EXTRACT_END_DAY}',1,6) as statis_end_month                      --新增订购周期的最后一个月份       
       ,phone_province_name
       ,order_phone_num
       ,channel_id
       ,business_id
       ,business_name
       ,sub_business_id
       ,sub_business_name
       ,product_id
       ,product_name
       ,order_crt_time
       ,order_program_id
       ,program_name
       ,order_opr_user_id       
       ,count(distinct new_add_order_user_id) as new_add_order_user_cnt                 --新增订购用户数      
       ,count(distinct one_day_retention_user_id) as one_day_retention_user_cnt         --某一天的留存用户数         
  from ( 
        select nvl(i.prov_name, nvl(j.prov_name, 'NA')) as phone_province_name  --省份
              ,a.order_phone_num
              ,a.channel_id                                                     --渠道id    
              ,a.business_id
              ,nvl(h1.business_name, 'NA') as business_name                     --业务名称
              ,a.sub_business_id              
              ,nvl(h2.sub_busi_name, 'NA') as sub_business_name                 --子业务名称 
              ,a.product_id
              ,nvl(m.product_name,'NA') as product_name
              ,a.order_crt_time
              ,a.order_program_id                                               
              ,nvl(n.program_name,'NA') as program_name
              ,a.order_opr_user_id
              ,a.order_user_id as new_add_order_user_id
              ,d1.order_user_id as one_day_retention_user_id                
          from tmp_new_add_order_user a          
          left join tmp_one_day_in_order_user d1 
            on (a.order_user_id = d1.order_user_id)   --？？？？？？？？？
            
          left join rptdata.dim_region i
            on (case when a.order_msisdn_region_id = '-998' then concat('', rand()) else a.order_msisdn_region_id end) = i.region_id
          left join rptdata.dim_region j
            on (case when a.payment_msisdn_region_id = '-998' then concat('', rand()) else a.payment_msisdn_region_id end) = j.region_id  
            
          left join rptdata.dim_server h1
            on (case when a.business_id = '-998' then concat('', rand()) else a.business_id end) = h1.business_id            
          left join rptdata.dim_server h2
            on (case when a.sub_business_id = '-998' then concat('', rand()) else a.sub_business_id end) = h2.sub_busi_id
          left join rptdata.dim_product m
            on (case when a.product_id = '-998' then concat('',rand()) else a.product_id end) = m.product_id
          left join rptdata.dim_program n
            on (case when a.order_program_id = '-998' then concat('',rand()) else a.order_program_id end) = n.program_id

       ) t 
 group by ${GROUP_BY_FIELDS} 

; 


