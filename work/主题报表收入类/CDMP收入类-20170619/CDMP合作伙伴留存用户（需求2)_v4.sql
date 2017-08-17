
with tmp_new_add_order_user as(                       --当前周期(如：456份，三个月)新增

select nvl(i.prov_name, 'NA') as phone_province_name  --省份  可选      
      ,a.order_phone_num                              --手机号码  可不导入     
      ,a.channel_id                                   --渠道ID    可不导入
      ,nvl(h.business_id, 'NA') as business_id        --业务ID    可不导入
      ,a.sub_business_id                              --子业务ID  可不导入     
      
      ,a.product_id                                   --产品ID
      ,a.order_crt_time                               --订购时间
      ,a.order_program_id                             --节目ID
      ,a.order_opr_user_id                            --操作员
      ,a.order_user_id      
  from rptdata.fact_order_item_detail a 
  left join rptdata.dim_sub_busi h
    on (case when a.sub_business_id = '-998' then concat('', rand()) else a.sub_business_id end) = h.sub_busi_id  	
  left join rptdata.dim_region i
    on (case when a.order_msisdn_region_id = '-998' then (case when a.payment_msisdn_region_id = '-998' then concat('',rand()) else a.payment_msisdn_region_id end) else a.order_msisdn_region_id end) = i.region_id	
 where a.src_file_day >= '20170601' 
   and a.src_file_day between '${EXTRACT_START_DAY}' and '${EXTRACT_END_DAY}' 
   and a.order_status in (5,9) 
   
   --限制相关维度
   and i.prov_name in ${PROVINCE_NAME}
   and a.order_phone_num in ${PHONE_NUM}
   and a.channel_id in ${CHANNEL_ID}
   and h.business_id in ${BUSINESS_ID}
   and a.sub_business_id in ${SUB_BUSINESS_ID}

   
 union all 

 
select nvl(i.prov_name,'NA') as phone_province_name      
      ,a.serv_number as order_phone_num     
      ,a.chn_id_new as channel_id 
      ,a.business_id
      ,a.sub_busi_id as sub_business_id      
      
      ,a.new_product_id as product_id      
      ,null as order_crt_time             --在源表中没有 不确定是不是 opr_time
      ,a.program_id as order_program_id
      ,null as order_opr_user_id          --在源表中没有 不确定是不是 opr_login
      ,b.user_id as order_user_id      
  from cdmp_dw.td_aaa_order_log_d a 
  join rptdata.dim_userid_usernum b
    on a.serv_number = b.user_num   
  left join rptdata.dim_region i 
    on (case when a.regoin_id = '-998' then concat('',rand()) else a.region_id end) = i.region_id
 where a.src_source_day <= '20170531'
   and a.src_source_day between '${EXTRACT_START_DAY}' and '${EXTRACT_END_DAY}'
   and a.order_type = 1   
   
   --相关维度的限定
   and i.prov_name in ${PROVINCE_NAME}
   and a.serv_number in ${PHONE_NUM}
   and a.chn_id_new in ${CHANNEL_ID}
   and a.business_id in ${BUSINESS_ID}
   and a.sub_busi_id in ${SUB_BUSINESS_ID}   
),

tmp_one_day_in_order_user as(       --某一天在订

select nvl(i.prov_name, 'NA') as phone_province_name     
      ,a.order_phone_num
      ,a.channel_id
      ,nvl(h.business_id, 'NA') as business_id  
      ,a.sub_business_id           
      ,a.product_id
      ,a.order_crt_time
      ,a.order_program_id
      ,a.order_opr_user_id
      ,a.order_user_id      
  from rptdata.fact_order_daily_snapshot a 
  left join rptdata.dim_sub_busi h
    on (case when a.sub_business_id = '-998' then concat('', rand()) else a.sub_business_id end) = h.sub_busi_id 
  left join rptdata.dim_region i
    on (case when a.order_msisdn_region_id = '-998' then (case when a.payment_msisdn_region_id = '-998' then concat('',rand()) else a.payment_msisdn_region_id end) else a.order_msisdn_region_id end) = i.region_id		
 where '${RETENTION_DAY}' >= '20170601'
   and a.snapshot_day = '${RETENTION_DAY}'
   
   --限制相关维度
   and i.prov_name in ${PROVINCE_NAME}
   and a.order_phone_num in ${PHONE_NUM}
   and a.channel_id in ${CHANNEL_ID}
   and h.business_id in ${BUSINESS_ID}
   and a.sub_business_id in ${SUB_BUSINESS_ID} 
 
 union all
 
select nvl(i.prov_name,'NA') as phone_province_name     
      ,a.serv_number as order_phone_num
      ,a.chn_id_new as channel_id
      ,a.business_id
      ,a.sub_busi_id as sub_business_id
      
      ,a.new_product_id as product_id
      ,null as order_crt_time              --在源表中没有 不确定是不是 opr_time
      ,null as order_program_id            --这个真没有
      ,null as order_opr_user_id           --在源表中没有 不确定是不是 opr_login
      ,b.user_id as order_user_id          
  from cdmp_dw.td_aaa_order_d a 
  join rptdata.dim_userid_usernum b
    on a.serv_number = b.user_num
  left join rptdata.dim_region i 
    on (case when a.regoin_id = '-998' then concat('',rand()) else a.region_id end) = i.region_id	
 where '${RETENTION_DAY}' <= '20170531'
   and a.src_source_day = '${RETENTION_DAY}' 
   
    --相关维度的限定
   and i.prov_name in ${PROVINCE_NAME}
   and a.serv_number in ${PHONE_NUM}
   and a.chn_id_new in ${CHANNEL_ID}
   and a.business_id in ${BUSINESS_ID}
   and a.sub_busi_id in ${SUB_BUSINESS_ID}  
)



select nvl(i.prov_name, nvl(j.prov_name, 'NA')) as phone_province_name  --省份
      ,order_phone_num
      ,channel_id
      ,business_id
	  ,nvl(h1.business_name, 'NA') as business_name 
      ,sub_business_id
	  ,nvl(h2.sub_busi_name, 'NA') as sub_business_name 
      ,product_id
	  ,nvl(m.product_name,'NA') as product_name
      ,order_crt_time
      ,order_program_id
	  ,nvl(n.program_name,'NA') as program_name
	  ,new_add_order_user_cnt
	  ,one_day_in_order_user_cnt as one_day_retention_user_cnt
  from (
		select phone_province_name
			  ,channel_id
			  ,business_id
			  ,sub_business_id
			  ,product_id
			  ,order_crt_time
			  ,order_program_id
			  ,sum(new_add_order_user_cnt) as new_add_order_user_cnt
			  ,sum(one_day_in_order_user_cnt) as one_day_in_order_user_cnt
		  from (
				select a.phone_province_name
					  ,a.order_phone_num
					  ,a.channel_id
					  ,a.business_id
					  ,a.sub_business_id
					  ,a.product_id
					  ,a.order_crt_time
					  ,a.order_program_id
					  ,count(distinct a.order_user_id) as new_add_order_user_cnt
					  ,0 as one_day_retention_user_cnt
				  from tmp_new_add_order_user a 
				  
				 union all
				 
				select d1.phone_province_name
					  ,d1.order_phone_num
					  ,d1.channel_id
					  ,d1.business_id
					  ,d1.sub_business_id
					  ,d1.product_id
					  ,d1.order_crt_time
					  ,d1.order_program_id
					  ,0 as new_add_order_user_cnt
					  ,count(d1.order_user_id) as one_day_in_order_user_cnt
				  from tmp_one_day_in_order_user d1
			   ) t 
		 group by phone_province_name
				 ,order_phone_num
				 ,channel_id
				 ,business_id
				 ,sub_business_id
				 ,product_id
				 ,order_crt_time
				 ,order_program_id
       ) tt  
  left join rptdata.dim_business h1
    on (case when tt.business_id = '-998' then concat('', rand()) else tt.business_id end)  = h1.business_id	
  left join rptdata.dim_sub_busi h2
    on (case when tt.sub_business_id = '-998' then concat('', rand()) else tt.sub_business_id end)  = h2.sub_busi_id
  left join rptdata.dim_product m
    on (case when tt.product_id = '-998' then concat('',rand()) else tt.product_id end) = m.product_id
  left join rptdata.dim_program n
    on (case when tt.order_program_id = '-998' then concat('',rand()) else tt.order_program_id end) = n.program_id
 where tt.new_add_order_user_cnt <> 0;	   
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 