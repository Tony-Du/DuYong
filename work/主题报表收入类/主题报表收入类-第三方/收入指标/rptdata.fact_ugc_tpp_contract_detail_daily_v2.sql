set mapreduce.job.name=rptdata.fact_ugc_tpp_order_relation_detail_${SRC_FILE_DAY};

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.exec.parallel=true;
set mapreduce.reduce.memory.mb=8192;
set mapreduce.reduce.java.opts=-Xmx7168m -XX:-UseGCOverheadLimit;

insert overwrite table rptdata.fact_ugc_tpp_order_relation_detail partition (src_file_day)
select  t.user_type
       ,t.network_type
       ,t.cp_id
       ,t.use_type
       ,t.term_prod_id      
       ,t.term_version_id   
       ,t.chn_id            
       ,t.city_id           
       ,t.sub_busi_id       
       ,t.user_id           
       ,t.period_unit       
       ,t.pay_chn_type      
       ,t.order_status      
       ,t.authorize_type    
       ,t.currency          
       ,t.company_id        
       ,t.goods_type        
       ,t.product_id        
       ,t.product_type      
       ,t.main              
       ,t.program_id        
       ,t.opr_time          
       ,t.substatus         
       ,t.order_id          
       ,t.payment_id        
       ,t.amount            
       ,t.payment_result    
       ,t.serv_number       
       ,t.sp_id             
       ,t.authorize_period  
       ,t.valid_start_time  
       ,t.expire_time       
       ,t.opr_login         
       ,t.totalamount
       ,t.period_amount
       ,t.src_file_day
  from (
        select a.user_type
              ,a.network_type
              ,a.cp_id
              ,a.use_type                  
              ,a.term_prod_id              
              ,a.term_version_id           
              ,a.chn_id                    
              ,a.city_id                   
              ,a.sub_busi_id               
              ,a.user_id           
              ,a.period_unit       
              ,a.pay_chn_type      
              ,a.order_status      
              ,a.authorize_type    
              ,a.currency          
              ,a.company_id        
              ,a.goods_type        
              ,a.product_id        
              ,a.product_type      
              ,a.main              
              ,a.program_id        
              ,a.opr_time          
              ,a.substatus         
              ,a.order_id          
              ,a.payment_id        
              ,a.amount            
              ,a.payment_result    
              ,a.serv_number       
              ,a.sp_id             
              ,a.authorize_period  
              ,unix_timestamp(a.valid_start_time, 'yyyyMMddHHmmss') as valid_start_time  
              ,case when a.expire_time in ('UNLIMITED', '19700101080000') then '4070880000' else unix_timestamp(a.expire_time, 'yyyyMMddHHmmss') end expire_time--2099-01-01 00:00:00
              ,a.opr_login        
              ,a.totalamount      
              ,case when a.period_unit = 'MONTH' then (a.totalamount / cast(a.authorize_period as int)) end as period_amount  --合约周期单价(包月分摊)       
              ,a.src_file_day                                           
          from rptdata.fact_ugc_order_detail_daily a 
         where a.pay_chn_type not in (50,58,59,302,304,306)     
           and a.main = 0
           and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')
           and a.order_status in ('5', '9')
           and a.src_file_day = '${SRC_FILE_DAY}'

         union all

        select b.user_type
              ,b.network_type
              ,b.cp_id
              ,b.use_type
              ,b.term_prod_id      
              ,b.term_version_id   
              ,b.chn_id            
              ,b.city_id           
              ,b.sub_busi_id       
              ,b.user_id           
              ,b.period_unit       
              ,b.pay_chn_type      
              ,b.order_status      
              ,b.authorize_type    
              ,b.currency          
              ,b.company_id        
              ,b.goods_type        
              ,b.product_id        
              ,b.product_type      
              ,b.main              
              ,b.program_id        
              ,b.opr_time          
              ,b.substatus         
              ,b.order_id          
              ,b.payment_id        
              ,b.amount            
              ,b.payment_result    
              ,b.serv_number       
              ,b.sp_id             
              ,b.authorize_period  
              ,b.valid_start_time  
              ,b.expire_time       
              ,b.opr_login         
              ,b.totalamount   
              ,b.period_amount
              ,'${SRC_FILE_DAY}' as src_file_day
          from rptdata.fact_ugc_tpp_order_relation_detail b
         where '${SRC_FILE_DAY}' <= b.expire_time
           and b.src_file_day = '${YESTERDAY}'     
       ) t ;