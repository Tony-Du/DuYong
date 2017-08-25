 
  
select company_id
      ,sum(accu_add_revenue) as accu_add_revenue
      ,sum(use_in_order_user_rate)/3 as use_in_order_user_rate 
      ,sum(mon_visit_user_cnt) as mon_accu_visit_user_cnt  
      ,sum(activ_visit)/3 as activ_visit                       
  from ( 
        select company_id
              ,accu_add_revenue
              ,0 as use_in_order_user_rate
              ,0 as mon_visit_user_cnt
              ,0 as activ_visit
          from qushupingtai.qspt_hzyykh_fcfj_accu_add_revenue a
         where a.src_file_month = '${QUARTER_THIRD_MONTH}'

         union all
         
        select company_id
              ,0 as accu_add_revenue
              ,use_in_order_user_rate
              ,0 as mon_visit_user_cnt
              ,0 as activ_visit
          from qushupingtai.qspt_hzyykh_fcfj_use_in_order_user_rate a 
         where a.src_file_month in ('${QUARTER_FIRST_MONTH}','${QUARTER_SECOND_MONTH}','${QUARTER_THIRD_MONTH}')
         
         union all
        
        select company_id
              ,0 as accu_add_revenue
              ,0 as use_in_order_user_rate
              ,mon_visit_user_cnt
              ,0 as activ_visit
           from qushupingtai.qspt_hzyykh_fcfj_mon_visit_user a 
          where a.src_file_month in ('${QUARTER_FIRST_MONTH}','${QUARTER_SECOND_MONTH}','${QUARTER_THIRD_MONTH}')    

          union all
          
        select company_id
              ,0 as accu_add_revenue
              ,0 as use_in_order_user_rate
              ,0 as mon_visit_user_cnt
              ,activ_visit
           from qushupingtai.qspt_hzyykh_fcfj_activ_visit a 
          where a.src_file_month in ('${QUARTER_FIRST_MONTH}','${QUARTER_SECOND_MONTH}','${QUARTER_THIRD_MONTH}')             
       ) t
 group by company_id;
 