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
              ,a.period_unit       --授权时间单位: hour(按次) day month(包周期)
              ,a.pay_chn_type      
              ,a.order_status      
              ,a.authorize_type    --授权类型：BOSS_MONTH, NULL, PERIOD, TIMES
              ,a.currency          
              ,a.company_id        
              ,a.goods_type        
              ,a.product_id        
              ,a.product_type      
              ,a.main              
              ,a.program_id        
              ,a.opr_time           --创建时间
              ,a.substatus         
              ,a.order_id          
              ,a.payment_id        
              ,a.amount             --intdata.ugc_order_paychannel_daily 中的 pay_amount
              ,a.payment_result    
              ,a.serv_number        --phone_number电话号码
              ,a.sp_id             
              ,a.authorize_period   --授权周期 intdata.ugc_order_item_daily 中的 amount
              ,a.valid_start_time  
              ,a.expire_time       
              ,a.opr_login          --phone_number电话号码
              ,a.totalamount,       --合约周期总价                                               
              ,case when period_unit = 'MONTH' then (a.totalamount / cast(a.authorize_period as int)) as period_amount  --合约周期单价(包月分摊)       
          from rptdata.fact_ugc_order_detail_daily a 
         where a.pay_chn_type not in (50,58,59,302,304,306)     --第三方
           and a.main = 0
           and a.goods_type not in ('MIGU_MOVIE_CARD','MOVIE_CARD_DELAY','MIGU_LIVE')
           and a.order_status in ('5', '9')
           and a.src_file_day = '20170519';  -- T
               
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
          from rptdata.fact_ugc_tpp_order_relation_detail b
         where '20170519' <= b.expire_time
           and b.src_file_day = '20170518'      -- T-1
       ) t ;