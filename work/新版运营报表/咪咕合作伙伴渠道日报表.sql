create or replace view .... as
select  t.dept_id            
       ,t.term_prod_id       
       ,t.term_video_type_id 
       ,t.term_video_soft_id 
       ,t.term_prod_type_id  
       ,t.term_version_id    
       ,t.term_os_type_id    
       ,t.busi_id            
       ,t.sub_busi_id        
       ,t.net_type_id        
       ,t.cp_id              
       ,t.product_id         
       ,t.content_id         
       ,t.content_type       
       ,t.broadcast_type_id  
       ,t.user_type_id       
       ,t.province_id        
       ,t.city_id            
       ,t.phone_province_id  
       ,t.phone_city_id      
       ,t.company_id         
       ,t.chn_id             
       ,t.chn_attr_1_id      
       ,t.chn_attr_2_id      
       ,t.chn_attr_3_id      
       ,t.chn_attr_4_id      
       ,t.chn_type           
       ,t.con_class_1_name   
       ,t.copyright_type     
       ,t.busi_type_id       
       ,t.program_id         

       ,sum(t.visit_cnt)                                        as visit_cnt           
       ,sum(t.use_cnt)                                          as use_cnt             
       ,sum(t.use_flow_kb)                                      as use_flow_kb         
       ,sum(t.use_flow_kb_i)                                    as use_flow_kb_i       
       ,sum(t.use_flow_kb_e)                                   as use_flow_kb_e       
       ,sum(t.use_duration)                                     as use_duration        
       ,sum(t.month_use_duration)                               as month_use_duration  
       ,sum(t.month_use_cnt)                                    as month_use_cnt       
       ,sum(t.time_use_cnt)                                     as time_use_cnt        
       ,sum(t.time_use_duration)                                as time_use_duration   
       ,sum(t.boss_month_add_info_fee_amount)                   as boss_month_add_info_fee_amount
       ,sum(t.boss_month_retention_info_amount)                 as boss_month_retention_info_amount
       ,sum(t.boss_time_amount)                                 as boss_time_amount       
       ,sum(t.boss_month_info_amount)                           as boss_month_info_amount 
       ,sum(t.boss_time_info_amount)                            as boss_time_info_amount  
       ,sum(t.third_prepay_amount)                              as third_prepay_amount    
       ,sum(t.real_amount)                                      as real_amount            
       ,sum(t.total_amount)                                     as total_amount           

       ,sum(t.visit_user_num)                                   as visit_user_num                                               
       ,sum(t.per_view_use_user_num)                            as per_view_use_user_num                                        
       ,sum(t.use_user_num)                                     as use_user_num                                                 
       ,sum(t.charge_use_user_num)                              as charge_use_user_num                                          
       ,sum(t.boss_time_add_order_user_num)                     as boss_time_add_order_user_num                                 
       ,sum(t.boss_month_in_order_user_num)                     as boss_month_in_order_user_num                                 
       ,sum(t.boss_month_add_order_user_num)                    as boss_month_add_order_user_num                                
       ,sum(t.boss_month_cancel_order_user_num)                 as boss_month_cancel_order_user_num                             
       ,sum(t.boss_month_add_and_cancel_user_num)               as boss_month_add_and_cancel_user_num                           
       ,sum(t.boss_month_pay_user_num)                          as boss_month_pay_user_num                                    
       ,sum(t.third_add_month_order_user_num)                   as third_add_month_order_user_num                               
       ,sum(t.third_add_time_order_user_num)                    as third_add_time_order_user_num                                
       ,sum(t.third_prepay_user_num)                            as third_prepay_user_num            
       ,sum(t.third_month_share_pay_user_num)                   as third_month_share_pay_user_num                               
       ,sum(t.third_pay_user_num)                               as third_pay_user_num                                         
       ,sum(t.add_month_order_user_num)                         as add_month_order_user_num                                   
       ,sum(t.add_time_order_user_num)                          as add_time_order_user_num                                
       ,sum(t.in_order_user_num)                                as in_order_user_num                                      
       ,sum(t.pay_user_num)                                     as pay_user_num                                           
       ,sum(t.boss_pay_user_num)                                as boss_pay_user_num                                      
       ,sum(t.boss_pay_active_user_num)                         as boss_pay_active_user_num                                     
       ,sum(t.prov_develop_boss_add_order_user_num)             as prov_develop_boss_add_order_user_num                         
       ,sum(t.natural_develop_boss_add_order_user_num)          as natural_develop_boss_add_order_user_num                      
       ,sum(t.prov_develop_boss_month_in_order_user_num)        as prov_develop_boss_month_in_order_user_num                    
       ,sum(t.natural_develop_boss_month_in_order_user_num)     as natural_develop_boss_month_in_order_user_num                 
       ,sum(t.idea_pay_user_num)                                as idea_pay_user_num                                            
       ,sum(t.mon_visit_user_num)                               as mon_visit_user_num                                           
       ,sum(t.mon_per_view_use_user_num)                        as mon_per_view_use_user_num                                    
       ,sum(t.mon_use_user_num)                                 as mon_use_user_num                                             
       ,sum(t.mon_charge_use_user_num)                          as mon_charge_use_user_num                                      
       ,sum(t.mon_boss_time_add_order_user_num)                 as mon_boss_time_add_order_user_num                             
       ,sum(t.mon_boss_month_in_order_user_num)                 as mon_boss_month_in_order_user_num                             
       ,sum(t.mon_boss_month_add_order_user_num)                as mon_boss_month_add_order_user_num                            
       ,sum(t.mon_boss_month_cancel_order_user_num)             as mon_boss_month_cancel_order_user_num                         
       ,sum(t.mon_boss_month_add_and_cancel_user_num)           as mon_boss_month_add_and_cancel_user_num                       
       ,sum(t.mon_boss_month_pay_user_num)                      as mon_boss_month_pay_user_num                                  
       ,sum(t.mon_third_add_month_order_user_num)               as mon_third_add_month_order_user_num                           
       ,sum(t.mon_third_add_time_order_user_num)                as mon_third_add_time_order_user_num                            
       ,sum(t.mon_third_prepay_user_num)                        as mon_third_prepay_user_num                    
       ,sum(t.mon_third_month_share_pay_user_num)               as mon_third_month_share_pay_user_num                           
       ,sum(t.mon_third_pay_user_num)                           as mon_third_pay_user_num                                     
       ,sum(t.mon_add_month_order_user_num)                     as mon_add_month_order_user_num                                 
       ,sum(t.mon_add_time_order_user_num)                      as mon_add_time_order_user_num                                  
       ,sum(t.mon_in_order_user_num)                            as mon_in_order_user_num                                        
       ,sum(t.mon_pay_user_num)                                 as mon_pay_user_num                                             
       ,sum(t.mon_boss_pay_user_num)                            as mon_boss_pay_user_num                                        
       ,sum(t.mon_boss_pay_active_user_num)                     as mon_boss_pay_active_user_num                                 
       ,sum(t.mon_prov_develop_boss_add_order_user_num)         as mon_prov_develop_boss_add_order_user_num                     
       ,sum(t.mon_natural_develop_boss_add_order_user_num)      as mon_natural_develop_boss_add_order_user_num                  
       ,sum(t.mon_prov_develop_boss_month_in_order_user_num)    as mon_prov_develop_boss_month_in_order_user_num                
       ,sum(t.mon_natural_develop_boss_month_in_order_user_num) as mon_natural_develop_boss_month_in_order_user_num             
       ,sum(t.mon_idea_pay_user_num)                            as mon_idea_pay_user_num                                        
       ,sum(t.three_month_new_visit_user_num)                   as three_month_new_visit_user_num                               
       ,sum(t.six_month_new_visit_user_num)                     as six_month_new_visit_user_num                                 
       ,sum(t.complain_user_num)                                as complain_user_num                                            
       ,sum(t.mon_complain_user_num)                            as mon_complain_user_num                                        
       ,sum(t.complain_num)                                     as complain_num                                                 
       ,sum(t.mon_complain_num)                                 as mon_complain_num                                             
       ,sum(t.new_complain_num)                                 as new_complain_num                                             
       ,sum(t.mon_new_complain_num)                             as mon_new_complain_num                                         
       ,sum(t.old_complain_num)                                 as old_complain_num                                             
       ,sum(t.mon_old_complain_num)                             as mon_old_complain_num                                         
       ,sum(t.unknown_complain_num)                             as unknown_complain_num                                         
       ,sum(t.mon_unknown_complain_num)                         as mon_unknown_complain_num                                     
       ,sum(t.fee_complain_num)                                 as fee_complain_num                              
       ,sum(t.mon_fee_complain_num)                             as mon_fee_complain_num                          
       ,sum(t.use_complain_num)                                 as use_complain_num                              
       ,sum(t.mon_use_complain_num)                             as mon_use_complain_num                          
       ,sum(t.new_unknown_complain_num)                         as new_unknown_complain_num                                     
       ,sum(t.mon_new_unknown_complain_num)                     as mon_new_unknown_complain_num                                 
       ,sum(t.old_unknown_complain_num)                         as old_unknown_complain_num                                     
       ,sum(t.mon_old_unknown_complain_num)                     as mon_old_unknown_complain_num                                 
       ,sum(t.free_use_user_num)                                as free_use_user_num                                      
       ,sum(t.mon_free_use_user_num)                            as mon_free_use_user_num                                  
       ,sum(t.month_use_user_num)                               as month_use_user_num                                     
       ,sum(t.mon_month_use_user_num)                           as mon_month_use_user_num                                     
  from ( 
        select a.dept_id            
              ,a.term_prod_id       
              ,a.term_video_type_id 
              ,a.term_video_soft_id 
              ,a.term_prod_type_id  
              ,a.term_version_id    
              ,a.term_os_type_id    
              ,a.busi_id            
              ,a.sub_busi_id        
              ,a.net_type_id        
              ,a.cp_id              
              ,a.product_id         
              ,a.content_id         
              ,a.content_type       
              ,a.broadcast_type_id  
              ,a.user_type_id       
              ,a.province_id        
              ,a.city_id            
              ,a.phone_province_id  
              ,a.phone_city_id      
              ,a.company_id         
              ,a.chn_id             
              ,a.chn_attr_1_id      
              ,a.chn_attr_2_id      
              ,a.chn_attr_3_id      
              ,a.chn_attr_4_id      
              ,a.chn_type           
              ,a.con_class_1_name   
              ,a.copyright_type     
              ,a.busi_type_id       
              ,a.program_id         
              
              ,a.visit_cnt           
              ,a.use_cnt             
              ,a.use_flow_kb         
              ,a.use_flow_kb_i       
              ,a.use_flow_kb_e       
              ,a.use_duration        
              ,a.month_use_duration  
              ,a.month_use_cnt       
              ,a.time_use_cnt        
              ,a.time_use_duration   
              ,a.boss_month_add_info_fee_amount
              ,a.boss_month_retention_info_amount
              ,a.boss_time_amount       
              ,a.boss_month_info_amount 
              ,a.boss_time_info_amount  
              ,a.third_prepay_amount    
              ,a.real_amount            
              ,a.total_amount           
              
              ,0 as visit_user_num                                              
              ,0 as per_view_use_user_num                                       
              ,0 as use_user_num                                                
              ,0 as charge_use_user_num                                         
              ,0 as boss_time_add_order_user_num                                        
              ,0 as boss_month_in_order_user_num                                        
              ,0 as boss_month_add_order_user_num                                       
              ,0 as boss_month_cancel_order_user_num                                    
              ,0 as boss_month_add_and_cancel_user_num                          
              ,0 as boss_month_pay_user_num                                 
              ,0 as third_add_month_order_user_num                              
              ,0 as third_add_time_order_user_num                               
              ,0 as third_prepay_user_num            
              ,0 as third_month_share_pay_user_num                              
              ,0 as third_pay_user_num                                      
              ,0 as add_month_order_user_num                                
              ,0 as add_time_order_user_num                             
              ,0 as in_order_user_num                                   
              ,0 as pay_user_num                                        
              ,0 as boss_pay_user_num                                   
              ,0 as boss_pay_active_user_num                                                    
              ,0 as prov_develop_boss_add_order_user_num                                        
              ,0 as natural_develop_boss_add_order_user_num                                 
              ,0 as prov_develop_boss_month_in_order_user_num                                   
              ,0 as natural_develop_boss_month_in_order_user_num                                        
              ,0 as idea_pay_user_num                                           
              ,0 as mon_visit_user_num                                          
              ,0 as mon_per_view_use_user_num                                        
              ,0 as mon_use_user_num                                            
              ,0 as mon_charge_use_user_num                                     
              ,0 as mon_boss_time_add_order_user_num                                        
              ,0 as mon_boss_month_in_order_user_num                                        
              ,0 as mon_boss_month_add_order_user_num                                       
              ,0 as mon_boss_month_cancel_order_user_num                                    
              ,0 as mon_boss_month_add_and_cancel_user_num                                  
              ,0 as mon_boss_month_pay_user_num                                     
              ,0 as mon_third_add_month_order_user_num                                  
              ,0 as mon_third_add_time_order_user_num                                   
              ,0 as mon_third_prepay_user_num                    
              ,0 as mon_third_month_share_pay_user_num                                  
              ,0 as mon_third_pay_user_num                                  
              ,0 as mon_add_month_order_user_num                                    
              ,0 as mon_add_time_order_user_num                                     
              ,0 as mon_in_order_user_num                                       
              ,0 as mon_pay_user_num                                            
              ,0 as mon_boss_pay_user_num                                       
              ,0 as mon_boss_pay_active_user_num                                        
              ,0 as mon_prov_develop_boss_add_order_user_num                                        
              ,0 as mon_natural_develop_boss_add_order_user_num                                     
              ,0 as mon_prov_develop_boss_month_in_order_user_num                                       
              ,0 as mon_natural_develop_boss_month_in_order_user_num                                        
              ,0 as mon_idea_pay_user_num                                       
              ,0 as three_month_new_visit_user_num                                      
              ,0 as six_month_new_visit_user_num                                        
              ,0 as complain_user_num                                           
              ,0 as mon_complain_user_num                                       
              ,0 as complain_num                                                
              ,0 as mon_complain_num                                            
              ,0 as new_complain_num                                            
              ,0 as mon_new_complain_num                                        
              ,0 as old_complain_num                                            
              ,0 as mon_old_complain_num                                        
              ,0 as unknown_complain_num                                        
              ,0 as mon_unknown_complain_num                                    
              ,0 as fee_complain_num                              
              ,0 as mon_fee_complain_num                          
              ,0 as use_complain_num                              
              ,0 as mon_use_complain_num                          
              ,0 as new_unknown_complain_num                                    
              ,0 as mon_new_unknown_complain_num                                        
              ,0 as old_unknown_complain_num                                    
              ,0 as mon_old_unknown_complain_num                                    
              ,0 as free_use_user_num                                   
              ,0 as mon_free_use_user_num                               
              ,0 as month_use_user_num                                  
              ,0 as mon_month_use_user_num                                  
                           
        --    ,a.dim_group_id  
              ,a.src_file_day           

          from rptdata.cdmp_rpt_cnt_daily a
         where a.dim_group_id = '15728769'
           
         union all 

        select  a.dept_id            
               ,a.term_prod_id       
               ,a.term_video_type_id 
               ,a.term_video_soft_id 
               ,a.term_prod_type_id  
               ,a.term_version_id    
               ,a.term_os_type_id    
               ,a.busi_id            
               ,a.sub_busi_id        
               ,a.net_type_id        
               ,a.cp_id              
               ,a.product_id         
               ,a.content_id         
               ,a.content_type       
               ,a.broadcast_type_id  
               ,a.user_type_id       
               ,a.province_id        
               ,a.city_id            
               ,a.phone_province_id  
               ,a.phone_city_id      
               ,a.company_id         
               ,a.chn_id             
               ,a.chn_attr_1_id      
               ,a.chn_attr_2_id      
               ,a.chn_attr_3_id      
               ,a.chn_attr_4_id      
               ,a.chn_type           
               ,a.con_class_1_name   
               ,a.copyright_type     
               ,a.busi_type_id       
               ,a.program_id          
               
               ,0 as visit_cnt           
               ,0 as use_cnt             
               ,0 as use_flow_kb         
               ,0 as use_flow_kb_i       
               ,0 as use_flow_kb_e       
               ,0 as use_duration        
               ,0 as month_use_duration  
               ,0 as month_use_cnt       
               ,0 as time_use_cnt        
               ,0 as time_use_duration   
               ,0 as boss_month_add_info_fee_amount
               ,0 as boss_month_retention_info_amount
               ,0 as boss_time_amount       
               ,0 as boss_month_info_amount 
               ,0 as boss_time_info_amount  
               ,0 as third_prepay_amount    
               ,0 as real_amount            
               ,0 as total_amount           
               
               ,a.visit_user_num                                            
               ,a.per_view_use_user_num                                     
               ,a.use_user_num                                              
               ,a.charge_use_user_num                                       
               ,a.boss_time_add_order_user_num                                      
               ,a.boss_month_in_order_user_num                                      
               ,a.boss_month_add_order_user_num                                     
               ,a.boss_month_cancel_order_user_num                                  
               ,a.boss_month_add_and_cancel_user_num                            
               ,a.boss_month_pay_user_num                                   
               ,a.third_add_month_order_user_num                                
               ,a.third_add_time_order_user_num                             
               ,a.third_prepay_user_num            
               ,a.third_month_share_pay_user_num                                
               ,a.third_pay_user_num                                    
               ,a.add_month_order_user_num                              
               ,a.add_time_order_user_num                               
               ,a.in_order_user_num                                 
               ,a.pay_user_num                                      
               ,a.boss_pay_user_num                                 
               ,a.boss_pay_active_user_num                                                  
               ,a.prov_develop_boss_add_order_user_num                                      
               ,a.natural_develop_boss_add_order_user_num                                   
               ,a.prov_develop_boss_month_in_order_user_num                                 
               ,a.natural_develop_boss_month_in_order_user_num                                      
               ,a.idea_pay_user_num                                         
               ,a.mon_visit_user_num                                        
               ,a.mon_per_view_use_user_num                                      
               ,a.mon_use_user_num                                          
               ,a.mon_charge_use_user_num                                       
               ,a.mon_boss_time_add_order_user_num                                      
               ,a.mon_boss_month_in_order_user_num                                      
               ,a.mon_boss_month_add_order_user_num                                     
               ,a.mon_boss_month_cancel_order_user_num                                  
               ,a.mon_boss_month_add_and_cancel_user_num                                    
               ,a.mon_boss_month_pay_user_num                                       
               ,a.mon_third_add_month_order_user_num                                
               ,a.mon_third_add_time_order_user_num                                 
               ,a.mon_third_prepay_user_num                  
               ,a.mon_third_month_share_pay_user_num                                
               ,a.mon_third_pay_user_num                                    
               ,a.mon_add_month_order_user_num                                  
               ,a.mon_add_time_order_user_num                                       
               ,a.mon_in_order_user_num                                     
               ,a.mon_pay_user_num                                          
               ,a.mon_boss_pay_user_num                                     
               ,a.mon_boss_pay_active_user_num                                      
               ,a.mon_prov_develop_boss_add_order_user_num                                      
               ,a.mon_natural_develop_boss_add_order_user_num                                       
               ,a.mon_prov_develop_boss_month_in_order_user_num                                     
               ,a.mon_natural_develop_boss_month_in_order_user_num                                      
               ,a.mon_idea_pay_user_num                                     
               ,a.three_month_new_visit_user_num                                        
               ,a.six_month_new_visit_user_num                                      
               ,a.complain_user_num                                         
               ,a.mon_complain_user_num                                     
               ,a.complain_num                                              
               ,a.mon_complain_num                                          
               ,a.new_complain_num                                          
               ,a.mon_new_complain_num                                      
               ,a.old_complain_num                                          
               ,a.mon_old_complain_num                                      
               ,a.unknown_complain_num                                      
               ,a.mon_unknown_complain_num                                  
               ,a.fee_complain_num                            
               ,a.mon_fee_complain_num                        
               ,a.use_complain_num                            
               ,a.mon_use_complain_num                        
               ,a.new_unknown_complain_num                                  
               ,a.mon_new_unknown_complain_num                                      
               ,a.old_unknown_complain_num                                  
               ,a.mon_old_unknown_complain_num                                  
               ,a.free_use_user_num                                 
               ,a.mon_free_use_user_num                             
               ,a.month_use_user_num                                
               ,a.mon_month_use_user_num       
               
               --,a.dim_group_id                       
               ,a.src_file_day          

          from rptdata.cdmp_rpt_user_num_daily a
         where a.dim_group_id = '15728769'
       ) t
 group by t.dept_id ,t.term_prod_id ,t.term_video_type_id ,t.term_video_soft_id ,t.term_prod_type_id  
          ,t.term_version_id ,t.term_os_type_id ,t.busi_id ,t.sub_busi_id ,t.net_type_id ,t.cp_id ,t.product_id         
          ,t.content_id ,t.content_type ,t.broadcast_type_id ,t.user_type_id ,t.province_id ,t.city_id            
          ,t.phone_province_id ,t.phone_city_id ,t.company_id ,t.chn_id ,t.chn_attr_1_id ,t.chn_attr_2_id      
          ,t.chn_attr_3_id ,t.chn_attr_4_id ,t.chn_type ,t.con_class_1_name ,t.copyright_type ,t.busi_type_id       
          ,t.program_id;         
  
       


15728769

--数据源
hive> desc cdmp_rpt_cnt_daily;
OK
dept_id                 string                                      
term_prod_id            string                                      
term_video_type_id      string                                      
term_video_soft_id      string                                      
term_prod_type_id       string                                      
term_version_id         string                                      
term_os_type_id         string                                      
busi_id                 string                                      
sub_busi_id             string                                      
net_type_id             string                                      
cp_id                   string                                      
product_id              string                                      
content_id              string                                      
content_type            string                                      
broadcast_type_id       string                                      
user_type_id            string                                      
province_id             string                                      
city_id                 string                                      
phone_province_id       string                                      
phone_city_id           string                                      
company_id              string                                      
chn_id                  string                                      
chn_attr_1_id           string                                      
chn_attr_2_id           string                                      
chn_attr_3_id           string                                      
chn_attr_4_id           string                                      
chn_type                string                                      
con_class_1_name        string                                      
copyright_type          string                                      
busi_type_id            string                                      
program_id              string
                                    
visit_cnt               bigint                                      
use_cnt                 bigint                                      
use_flow_kb             bigint                                      
use_flow_kb_i           bigint                                      
use_flow_kb_e           bigint                                      
use_duration            bigint                                      
month_use_duration      bigint                                      
month_use_cnt           bigint                                      
time_use_cnt            bigint                                      
time_use_duration       bigint                                      
boss_month_add_info_fee_amount  decimal(38,4)                               
boss_month_retention_info_amount    decimal(38,4)                               
boss_time_amount        decimal(38,4)                               
boss_month_info_amount  decimal(38,4)                               
boss_time_info_amount   decimal(38,4)                               
third_prepay_amount     decimal(38,4)                               
real_amount             decimal(38,4)                               
total_amount            decimal(38,4)                               
dim_group_id            bigint                                      
src_file_day            string

====================================================================================================================================================================

hive> desc cdmp_rpt_cnt_monthly;
OK
dept_id                 string                                      
term_prod_id            string                                      
term_video_type_id      string                                      
term_video_soft_id      string                                      
term_prod_type_id       string                                      
term_version_id         string                                      
term_os_type_id         string                                      
busi_id                 string                                      
sub_busi_id             string                                      
net_type_id             string                                      
cp_id                   string                                      
product_id              string                                      
content_id              string                                      
content_type            string                                      
broadcast_type_id       string                                      
user_type_id            string                                      
province_id             string                                      
city_id                 string                                      
phone_province_id       string                                      
phone_city_id           string                                      
company_id              string                                      
chn_id                  string                                      
chn_attr_1_id           string                                      
chn_attr_2_id           string                                      
chn_attr_3_id           string                                      
chn_attr_4_id           string                                      
chn_type                string                                      
con_class_1_name        string                                      
copyright_type          string                                      
busi_type_id            string                                      
program_id              string                                      
visit_cnt               bigint                                      
use_cnt                 bigint                                      
use_flow_kb             bigint                                      
use_flow_kb_i           bigint                                      
use_flow_kb_e           bigint                                      
use_duration            bigint                                      
month_use_duration      bigint                                      
month_use_cnt           bigint                                      
time_use_cnt            bigint                                      
time_use_duration       bigint                                      
boss_month_add_info_fee_amount  decimal(38,4)                               
boss_month_retention_info_amount    decimal(38,4)                               
boss_time_amount        decimal(38,4)                               
boss_month_info_amount  decimal(38,4)                               
boss_time_info_amount   decimal(38,4)                               
third_prepay_amount     decimal(38,4)                               
real_amount             decimal(38,4)                               
total_amount            decimal(38,4)                               
dim_group_id            bigint                                      
src_file_month          string 

====================================================================================================================================================================

hive> desc cdmp_rpt_user_num_daily;
OK
dept_id                 string                                      
term_prod_id            string                                      
term_video_type_id      string                                      
term_video_soft_id      string                                      
term_prod_type_id       string                                      
term_version_id         string                                      
term_os_type_id         string                                      
busi_id                 string                                      
sub_busi_id             string                                      
net_type_id             string                                      
cp_id                   string                                      
product_id              string                                      
content_id              string                                      
content_type            string                                      
broadcast_type_id       string                                      
user_type_id            string                                      
province_id             string                                      
city_id                 string                                      
phone_province_id       string                                      
phone_city_id           string                                      
company_id              string                                      
chn_id                  string                                      
chn_attr_1_id           string                                      
chn_attr_2_id           string                                      
chn_attr_3_id           string                                      
chn_attr_4_id           string                                      
chn_type                string                                      
con_class_1_name        string                                      
copyright_type          string                                      
busi_type_id            string                                      
program_id              string
                                    
visit_user_num          bigint                                      
per_view_use_user_num   bigint                                      
use_user_num            bigint                                      
charge_use_user_num     bigint                                      
boss_time_add_order_user_num    bigint                                      
boss_month_in_order_user_num    bigint                                      
boss_month_add_order_user_num   bigint                                      
boss_month_cancel_order_user_num    bigint                                      
boss_month_add_and_cancel_user_num  bigint                                      
boss_month_pay_user_num bigint                                      
third_add_month_order_user_num  bigint                                      
third_add_time_order_user_num   bigint                                      
third_prepay_user_num   bigint                                      
third_month_share_pay_user_num  bigint                                      
third_pay_user_num      bigint                                      
add_month_order_user_num    bigint                                      
add_time_order_user_num bigint                                      
in_order_user_num       bigint                                      
pay_user_num            bigint                                      
boss_pay_user_num       bigint                                      
boss_pay_active_user_num    bigint                                      
prov_develop_boss_add_order_user_num    bigint                                      
natural_develop_boss_add_order_user_num bigint                                      
prov_develop_boss_month_in_order_user_num   bigint                                      
natural_develop_boss_month_in_order_user_num    bigint                                      
idea_pay_user_num       bigint                                      
mon_visit_user_num      bigint                                      
mon_per_view_use_user_num   bigint                                       
mon_use_user_num        bigint                                      
mon_charge_use_user_num bigint                                      
mon_boss_time_add_order_user_num    bigint                                      
mon_boss_month_in_order_user_num    bigint                                      
mon_boss_month_add_order_user_num   bigint                                      
mon_boss_month_cancel_order_user_num    bigint                                      
mon_boss_month_add_and_cancel_user_num  bigint                                      
mon_boss_month_pay_user_num bigint                                      
mon_third_add_month_order_user_num  bigint                                      
mon_third_add_time_order_user_num   bigint                                      
mon_third_prepay_user_num   bigint                                      
mon_third_month_share_pay_user_num  bigint                                      
mon_third_pay_user_num  bigint                                      
mon_add_month_order_user_num    bigint                                      
mon_add_time_order_user_num bigint                                      
mon_in_order_user_num   bigint                                      
mon_pay_user_num        bigint                                      
mon_boss_pay_user_num   bigint                                      
mon_boss_pay_active_user_num    bigint                                      
mon_prov_develop_boss_add_order_user_num    bigint                                      
mon_natural_develop_boss_add_order_user_num bigint                                      
mon_prov_develop_boss_month_in_order_user_num   bigint                                      
mon_natural_develop_boss_month_in_order_user_num    bigint                                      
mon_idea_pay_user_num   bigint                                      
three_month_new_visit_user_num  bigint                                      
six_month_new_visit_user_num    bigint                                      
complain_user_num       bigint                                      
mon_complain_user_num   bigint                                      
complain_num            bigint                                      
mon_complain_num        bigint                                      
new_complain_num        bigint                                      
mon_new_complain_num    bigint                                      
old_complain_num        bigint                                      
mon_old_complain_num    bigint                                      
unknown_complain_num    bigint                                      
mon_unknown_complain_num    bigint                                      
fee_complain_num        bigint                                      
mon_fee_complain_num    bigint                                      
use_complain_num        bigint                                      
mon_use_complain_num    bigint                                      
new_unknown_complain_num    bigint                                      
mon_new_unknown_complain_num    bigint                                      
old_unknown_complain_num    bigint                                      
mon_old_unknown_complain_num    bigint                                      
free_use_user_num       bigint                                      
mon_free_use_user_num   bigint                                      
month_use_user_num      bigint                                      
mon_month_use_user_num  bigint                                      
dim_group_id            bigint                                      
src_file_day            string 

====================================================================================================================================================================

hive> desc cdmp_rpt_user_num_monthly;
OK
dept_id                 string                                      
term_prod_id            string                                      
term_video_type_id      string                                      
term_video_soft_id      string                                      
term_prod_type_id       string                                      
term_version_id         string                                      
term_os_type_id         string                                      
busi_id                 string                                      
sub_busi_id             string                                      
net_type_id             string                                      
cp_id                   string                                      
product_id              string                                      
content_id              string                                      
content_type            string                                      
broadcast_type_id       string                                      
user_type_id            string                                      
province_id             string                                      
city_id                 string                                      
phone_province_id       string                                      
phone_city_id           string                                      
company_id              string                                      
chn_id                  string                                      
chn_attr_1_id           string                                      
chn_attr_2_id           string                                      
chn_attr_3_id           string                                      
chn_attr_4_id           string                                      
chn_type                string                                      
con_class_1_name        string                                      
copyright_type          string                                      
busi_type_id            string                                      
program_id              string                                      
visit_user_num          bigint                                      
per_view_use_user_num   bigint                                      
use_user_num            bigint                                      
charge_use_user_num     bigint                                      
boss_time_add_order_user_num    bigint                                      
boss_month_in_order_user_num    bigint                                      
boss_month_add_order_user_num   bigint                                      
boss_month_cancel_order_user_num    bigint                                      
boss_month_add_and_cancel_user_num  bigint                                      
boss_month_pay_user_num bigint                                      
third_add_month_order_user_num  bigint                                      
third_add_time_order_user_num   bigint                                      
third_prepay_user_num   bigint                                      
third_month_share_pay_user_num  bigint                                      
third_pay_user_num      bigint                                      
add_month_order_user_num    bigint                                      
add_time_order_user_num bigint                                      
in_order_user_num       bigint                                      
pay_user_num            bigint                                      
boss_pay_user_num       bigint                                      
boss_pay_active_user_num    bigint                                      
prov_develop_boss_add_order_user_num    bigint                                      
natural_develop_boss_add_order_user_num bigint                                      
prov_develop_boss_month_in_order_user_num   bigint                                      
natural_develop_boss_month_in_order_user_num    bigint                                      
idea_pay_user_num       bigint                                      
three_month_new_visit_user_num  bigint                                      
six_month_new_visit_user_num    bigint                                      
complain_user_num       bigint                                      
complain_num            bigint                                      
new_complain_num        bigint                                      
old_complain_num        bigint                                      
unknown_complain_num    bigint                                      
fee_complain_num        bigint                                      
use_complain_num        bigint                                      
new_unknown_complain_num    bigint                                      
old_unknown_complain_num    bigint                                      
free_use_user_num       bigint                                      
month_use_user_num      bigint                                      
dim_group_id            bigint                                      
src_file_month          string 