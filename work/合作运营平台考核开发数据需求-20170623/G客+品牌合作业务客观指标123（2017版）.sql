with busi_sub_busi as (
select a.business_id
      ,a.sub_busi_id
  from rptdata.dim_sub_busi a
  join (select distinct business_id from ${DIM_BUSI_ID_TBL}) b
    on a.business_id = b.business_id
 group by a.business_id
         ,a.sub_busi_id 
)
insert overwrite table cdmpview.qspt_hzyykh_17_check_1_2 partition (src_file_month = '${SRC_FILE_MONTH}')
select substr('${MONTH_START_DAY}', 1, 6) statis_month
      ,b.business_id
      ,fud.usernum_id
      ,sum(fud.flow_kb) as flow_kb
      ,sum(fud.duration_sec) as duration_sec 
  from rptdata.fact_use_detail fud
  join busi_sub_busi b 
    on fud.sub_busi_id = b.sub_busi_id 
 where fud.src_file_day >= '${MONTH_START_DAY}' 
   and fud.src_file_day <= '${MONTH_END_DAY}'
   and fud.user_type_id = '1'
 group by b.business_id
         ,fud.usernum_id;


         
with busi_sub_busi as (
select a.business_id
      ,a.sub_busi_id
  from rptdata.dim_sub_busi a
  join (select distinct business_id from ${DIM_BUSI_ID_TBL}) b 
    on a.business_id = b.business_id
 group by a.business_id
         ,a.sub_busi_id 
)
insert overwrite table cdmpview.qspt_hzyykh_17_check_3 partition (src_file_month = '${SRC_FILE_MONTH}')
select substr('${MONTH_START_DAY}',1,6) as statis_month
      ,a.business_id
      ,sum(case when a.rn <= 3 then a.check_3_u_num else 0 end) as in_3_check_num 
      ,sum(a.check_3_u_num) as all_check_3_num
  from (
        select t.business_id
              ,t.use_source_ip
              ,sum(t.check_3_u_num) as check_3_u_num
              ,row_number() over (partition by t.business_id order by sum(t.check_3_u_num) desc) rn
          from (     
                select a1.src_file_day
                      ,a1.business_id
                      ,a1.use_source_ip   
                      ,count(a1.usernum_id) as check_3_u_num 
                  from (
                        select fud.src_file_day
                              ,b.business_id
                              ,fud.use_source_ip
                              ,fud.usernum_id                             
                          from rptdata.fact_use_detail fud
                          join busi_sub_busi b
                            on fud.sub_busi_id = b.sub_busi_id 
                         where fud.src_file_day >= '${MONTH_START_DAY}' 
                           and fud.src_file_day <= '${MONTH_END_DAY}' 
                           and fud.user_type_id = '1' 
                         group by fud.src_file_day
                                 ,b.business_id
                                 ,fud.use_source_ip
                                 ,fud.usernum_id
                       ) a1 
                 group by a1.src_file_day
                         ,a1.business_id
                         ,a1.use_source_ip                  
               ) t               
        group by t.business_id
                ,t.use_source_ip         
       ) a
group by a.business_id;



with busi_sub_busi as (
select a.business_id
      ,a.sub_busi_id
  from rptdata.dim_sub_busi a
  join (select distinct business_id from ${DIM_BUSI_ID_TBL}) b
    on a.business_id = b.business_id
 group by a.business_id
         ,a.sub_busi_id 
)
insert overwrite table cdmpview.qspt_hzyykh_17_check_4 partition (src_file_month = '${SRC_FILE_MONTH}')
select substr('${MONTH_START_DAY}',1,6) as statis_month
      ,a.business_id
      ,sum(case when a.rn <= 3 then a.check_4_num else 0 end) as in_3_check_num 
      ,sum(a.check_4_num) as all_check_4_num
  from (
        select t.business_id
              ,t.client_ip
              ,sum(t.check_4_num) as check_4_num
              ,row_number() over (partition by t.business_id order by sum(t.check_4_num) desc) rn
          from (     
                select a1.src_file_day
                      ,a1.business_id
                      ,a1.client_ip                        
                      ,count(a1.user_key) as check_4_num 
                  from ( 
                         select fuvh.src_file_day
                               ,b.business_id
                               ,fuvh.client_ip
                               ,fuvh.user_key                              
                           from rptdata.fact_user_visit_hourly fuvh
                           join busi_sub_busi b
                             on fuvh.sub_busi_id = b.sub_busi_id 
                          where fuvh.src_file_day >= '${MONTH_START_DAY}' 
                            and fuvh.src_file_day <= '${MONTH_END_DAY}'
                          group by fuvh.src_file_day
                                  ,b.business_id
                                  ,fuvh.client_ip
                                  ,fuvh.user_key
                        ) a1
                 group by a1.src_file_day
                         ,a1.business_id
                         ,a1.client_ip 
               ) t               
        group by t.business_id
                ,t.client_ip         
       ) a
group by a.business_id;



with busi_sub_busi as (
select a.business_id
      ,a.sub_busi_id
  from rptdata.dim_sub_busi a
  join (select distinct business_id from ${DIM_BUSI_ID_TBL}) b
    on a.business_id = b.business_id
 group by a.business_id
         ,a.sub_busi_id 
)
insert overwrite table cdmpview.qspt_hzyykh_17_assess_1 partition (src_file_month = '${SRC_FILE_MONTH}')
select substr('${MONTH_END_DAY}', 1, 6) as statis_month
      ,t.business_id
      ,sum(t.accu_add_revenue) as accu_add_revenue
  from (      
        select b.business_id
              ,sum(a.payment_amt) as accu_add_revenue
          from rptdata.fact_order_item_detail a
          join busi_sub_busi b
            on a.sub_business_id = b.sub_busi_id
         where a.src_file_day >= '20170601' and a.src_file_day <= '${MONTH_END_DAY}' 
           and a.order_status in (5,9)
           and a.boss_repeat_order_flag <> 'Y'
           and a.boss_last_success_bill_flag <> 'N'
         group by b.business_id
         
         union all 
         
        select ar.business_id
              ,ar.accu_add_revenue
          from cdmpview.qspt_hzyykh_201704_05_add_revenue ar
          join (select distinct business_id from ${DIM_BUSI_ID_TBL}) b
            on ar.business_id = b.business_id
         group by ar.business_id
                 ,ar.accu_add_revenue
       ) t
 group by t.business_id;
 
       
       
with busi_sub_busi as (
select a.business_id
      ,a.sub_busi_id
  from rptdata.dim_sub_busi a
  join (select distinct business_id from ${DIM_BUSI_ID_TBL}) b 
    on a.business_id = b.business_id
 group by a.business_id
         ,a.sub_busi_id 
)
insert overwrite table cdmpview.qspt_hzyykh_17_assess_2_u partition (src_file_month = '${SRC_FILE_MONTH}')
select substr('${MONTH_START_DAY}', 1, 6) as statis_month
      ,a.business_id
      ,count(a.usernum_id) as use_user_cnt 
  from (
        select case when tp.term_prod_name in ('和视频OPENAPI', '和视频SDK') then nvl(dc.dept_name,'9999') 
                    else nvl(tp.dept_name, nvl(bu.dept, nvl(dc.dept_name, '9999'))) end as dept 
              ,b.business_id
              ,fud.usernum_id 
          from rptdata.fact_use_detail fud 
          join busi_sub_busi b 
            on fud.sub_busi_id = b.sub_busi_id
          left join rptdata.dim_dept_term_prod tp
            on fud.termprod_id = tp.term_prod_id 
          left join cdmpview.tmp_wsj_0606_dim_busi_new bu 
            on b.business_id = bu.business_id
          left join rptdata.dim_dept_chn dc
            on fud.channel_id = dc.chn_id 
         where fud.src_file_day >= '${MONTH_START_DAY}' 
           and fud.src_file_day <= '${MONTH_END_DAY}'
           and fud.user_type_id = '1'
         group by case when tp.term_prod_name in ('和视频OPENAPI', '和视频SDK') then nvl(dc.dept_name,'9999') 
                       else nvl(tp.dept_name, nvl(bu.dept, nvl(dc.dept_name, '9999')))  end
                 ,b.business_id
                 ,fud.usernum_id         
       ) a
  join cdmpview.qspt_hzyykh_17_check_1_2 b
    on a.usernum_id = b.usernum_id and a.business_id = b.business_id and b.src_file_month = '${SRC_FILE_MONTH}'
 where a.dept = '合作运营部' 
   and b.flow_kb >= ${FLOW_THRESHOLD_KB}
   and b.duration_sec >= ${DURATION_THRESHOLD_SEC} 
 group by a.business_id;



with busi_sub_busi as (
select a.business_id
      ,a.sub_busi_id
  from rptdata.dim_sub_busi a
  join (select distinct business_id from ${DIM_BUSI_ID_TBL}) b 
    on a.business_id = b.business_id
 group by a.business_id
         ,a.sub_busi_id 
)
insert overwrite table cdmpview.qspt_hzyykh_17_assess_2_order partition (src_file_month = '${SRC_FILE_MONTH}')      
select substr('${MONTH_END_DAY}', 1, 6) as statis_month
      ,t.business_id 
      ,count(t.order_user_id) as in_order_user_cnt
  from (
        select b.business_id
              ,a.order_user_id   
          from rptdata.fact_order_daily_snapshot a
          join rptdata.dim_charge_product c
            on a.product_id = c.chrgprod_id 
          join busi_sub_busi b
            on c.sub_busi_bdid = b.sub_busi_id 
         where a.snapshot_day = '${MONTH_END_DAY}' 
           and c.chrgprod_price > 0 
         group by b.business_id,
                  a.order_user_id
       ) t
 group by t.business_id;


 
insert overwrite TABLE cdmpview.qspt_hzyykh_17_assess_2 partition (src_file_month = '${SRC_FILE_MONTH}')
select t.statis_month
      ,t.business_id
      ,sum(t.use_user_cnt) as use_user_cnt
      ,sum(t.in_order_user_cnt) as in_order_user_cnt
      ,case when sum(t.in_order_user_cnt) = 0 then 0 else sum(t.use_user_cnt)/sum(t.in_order_user_cnt) end as use_in_order_user_rate
  from (
        select u.statis_month
              ,u.business_id
              ,u.use_user_cnt
              ,0 as in_order_user_cnt
          from cdmpview.qspt_hzyykh_17_assess_2_u u 
         where u.src_file_month = '${SRC_FILE_MONTH}'
         
         union all 
         
        select o.statis_month
              ,o.business_id
              ,0 as use_user_cnt
              ,o.in_order_user_cnt
          from cdmpview.qspt_hzyykh_17_assess_2_order o
         where o.src_file_month = '${SRC_FILE_MONTH}' 
       ) t  
group by t.statis_month, t.business_id

---SELECT nvl(u.statis_month, o.statis_month ) statis_month
---      ,nvl(u.business_id, o.business_id) business_id
---      ,nvl(u.use_user_cnt, 0) use_user_cnt
---      ,nvl(o.in_order_user_cnt, 0) in_order_user_cnt
---      ,case when nvl(o.in_order_user_cnt, 0) = 0 then 0 else nvl(u.use_user_cnt, 0)/o.in_order_user_cnt end as use_in_order_user_rate
---  FROM cdmpview.qspt_hzyykh_17_assess_2_u u 
---  FULL JOIN cdmpview.qspt_hzyykh_17_assess_2_order o
---    ON (u.business_id = o.business_id and o.src_file_month = '${SRC_FILE_MONTH}')
--- where u.src_file_month = '${SRC_FILE_MONTH}';



with busi_sub_busi as (
select a.business_id
      ,a.sub_busi_id
  from rptdata.dim_sub_busi a
  join (select distinct business_id from ${DIM_BUSI_ID_TBL}) b
    on a.business_id = b.business_id
 group by a.business_id
         ,a.sub_busi_id 
),
assess_3_detail as (
select case when tp.term_prod_name in ('和视频OPENAPI', '和视频SDK') then nvl(dc.dept_name,'9999') 
            else nvl(tp.dept_name, nvl(bu.dept, nvl(dc.dept_name, '9999'))) end as dept 
      ,fuvh.src_file_day
      ,b.business_id
      ,fuvh.user_key
      ,fuvh.user_type_id
  from rptdata.fact_user_visit_hourly fuvh 
  join busi_sub_busi b
    on fuvh.sub_busi_id = b.sub_busi_id 
  left join rptdata.dim_dept_term_prod tp
    on fuvh.term_prod_id = tp.term_prod_id 
  left join cdmpview.tmp_wsj_0606_dim_busi_new bu 
    on b.business_id = bu.business_id
  left join rptdata.dim_dept_chn dc
    on fuvh.chn_id = dc.chn_id 
 where fuvh.src_file_day >= '${MONTH_START_DAY}' 
   and fuvh.src_file_day <= '${MONTH_END_DAY}' 
 group by case when tp.term_prod_name in ('和视频OPENAPI', '和视频SDK') then nvl(dc.dept_name,'9999') 
            else nvl(tp.dept_name, nvl(bu.dept, nvl(dc.dept_name, '9999'))) end  
         ,fuvh.src_file_day
         ,b.business_id
         ,fuvh.user_key
         ,fuvh.user_type_id
)
insert overwrite table cdmpview.qspt_hzyykh_17_assess_3_d partition (src_file_month = '${SRC_FILE_MONTH}')
select substr('${MONTH_START_DAY}', 1, 6) as statis_month
      ,t.business_id 
      ,sum(t.visit_user_cnt)/cast(substr('${MONTH_END_DAY}',7,2) as int) as day_avg_visit_user_cnt 
  from (
        select a.src_file_day
              ,a.business_id           
              ,count(distinct a.user_key) visit_user_cnt
          from assess_3_detail a         
         where a.dept = '合作运营部' 
         group by a.src_file_day
                 ,a.business_id
                 --,a.user_type_id
       ) t
 group by t.business_id;


 
with busi_sub_busi as (
select a.business_id
      ,a.sub_busi_id
  from rptdata.dim_sub_busi a
  join (select distinct business_id from ${DIM_BUSI_ID_TBL}) b
    on a.business_id = b.business_id
 group by a.business_id
         ,a.sub_busi_id 
),
assess_3_detail as (
select case when tp.term_prod_name in ('和视频OPENAPI', '和视频SDK') then nvl(dc.dept_name,'9999') 
            else nvl(tp.dept_name, nvl(bu.dept, nvl(dc.dept_name, '9999'))) end as dept 
      ,fuvh.src_file_day
      ,b.business_id
      ,fuvh.user_key
      ,fuvh.user_type_id
  from rptdata.fact_user_visit_hourly fuvh 
  join busi_sub_busi b
    on fuvh.sub_busi_id = b.sub_busi_id 
  left join rptdata.dim_dept_term_prod tp
    on fuvh.term_prod_id = tp.term_prod_id 
  left join cdmpview.tmp_wsj_0606_dim_busi_new bu
    on b.business_id = bu.business_id
  left join rptdata.dim_dept_chn dc
    on fuvh.chn_id = dc.chn_id 
 where fuvh.src_file_day >= '${MONTH_START_DAY}' 
   and fuvh.src_file_day <= '${MONTH_END_DAY}' 
 group by case when tp.term_prod_name in ('和视频OPENAPI', '和视频SDK') then nvl(dc.dept_name,'9999') 
            else nvl(tp.dept_name, nvl(bu.dept, nvl(dc.dept_name, '9999'))) end  
         ,fuvh.src_file_day
         ,b.business_id
         ,fuvh.user_key
         ,fuvh.user_type_id
)
insert overwrite table cdmpview.qspt_hzyykh_17_assess_3_m partition (src_file_month = '${SRC_FILE_MONTH}')
select substr('${MONTH_START_DAY}', 1, 6) as statis_month
      ,t.business_id
      ,sum(t.login_visit_user_cnt) as login_visit_user_cnt 
      ,sum(t.visit_user_cnt) as visit_user_cnt 
  from (    
        select a.business_id
              ,case when a.user_type_id = '1' then count(distinct a.user_key) else 0 end as login_visit_user_cnt
              ,0 as visit_user_cnt 
          from assess_3_detail a         
         where a.dept = '合作运营部'
         group by a.business_id
                 ,a.user_type_id
          
         union all
         
         select a.business_id
              ,0 as login_visit_user_cnt
              ,count(distinct a.user_key) visit_user_cnt 
          from assess_3_detail a         
         where a.dept = '合作运营部'
         group by a.business_id         
       ) t
 group by t.business_id ;
 
 
 
insert overwrite TABLE cdmpview.qspt_hzyykh_17_assess_3 partition (src_file_month = '${SRC_FILE_MONTH}')
select t.statis_month
      ,t.business_id
      ,sum(t.day_avg_visit_user_cnt) as day_avg_visit_user_cnt
      ,sum(t.login_visit_user_cnt) as login_visit_user_cnt
      ,sum(t.visit_user_cnt) as visit_user_cnt
      ,case when sum(t.visit_user_cnt) = 0 then 0 
            else sum(t.day_avg_visit_user_cnt)/sum(t.visit_user_cnt) end as activ_visit 
      ,case when sum(t.visit_user_cnt) = 0 then 0 
            else 1 - (sum(t.login_visit_user_cnt)/sum(t.visit_user_cnt)) end as tourist_rate      
  from (
        select d.statis_month
              ,d.business_id
              ,d.day_avg_visit_user_cnt
              ,0 as login_visit_user_cnt
              ,0 as visit_user_cnt
          from cdmpview.qspt_hzyykh_17_assess_3_d d
         where d.src_file_month = '${SRC_FILE_MONTH}'

         union all 
         
        select m.statis_month
              ,m.business_id
              ,0 as day_avg_visit_user_cnt
              ,m.login_visit_user_cnt
              ,m.visit_user_cnt
          from cdmpview.qspt_hzyykh_17_assess_3_m m 
         where m.src_file_month = '${SRC_FILE_MONTH}'
       ) t
 group by t.statis_month, t.business_id


--select nvl(d.statis_month, m.statis_month) as statis_month
--      ,nvl(d.business_id, m.business_id) as business_id
--      ,nvl(d.day_avg_visit_user_cnt, 0) as day_avg_visit_user_cnt
--      ,nvl(m.login_visit_user_cnt, 0) as login_visit_user_cnt   
--      ,nvl(m.visit_user_cnt, 0) as visit_user_cnt
--      ,case when nvl(m.visit_user_cnt, 0) = 0 then 0 
--            else nvl(d.day_avg_visit_user_cnt, 0)/m.visit_user_cnt end as activ_visit 
--      ,case when nvl(m.visit_user_cnt, 0) = 0 then 0 
--            else 1 - (nvl(m.login_visit_user_cnt, 0)/m.visit_user_cnt) end as tourist_rate 
--  from cdmpview.qspt_hzyykh_17_assess_3_d d
--  full join cdmpview.qspt_hzyykh_17_assess_3_m m 
--    on (d.business_id = m.business_id and d.src_file_month = '${SRC_FILE_MONTH}')
-- where m.src_file_month = '${SRC_FILE_MONTH}';

 
--${OUTPUT_RESULT}
--select  nvl(a1.statis_month,a2.statis_month) as statis_month
--       ,nvl(a1.business_id, a2.business_id) as business_id
--       ,nvl(a1.accu_add_revenue, 0) as accu_add_revenue
--       ,nvl(a2.use_user_cnt, 0) as use_user_cnt
--       ,nvl(a2.in_order_user_cnt, 0) as in_order_user_cnt
--       ,nvl(a2.use_in_order_user_rate, 0) as use_in_order_user_rate 
--       ,nvl(a3.activ_visit, 0) as activ_visit   
--       ,nvl(a3.tourist_rate, 0) as tourist_rate                     
--       ,case when c3.all_check_3_num = 0 then 0 
--             else c3.in_3_check_num/c3.all_check_3_num  
--              end as propo_3_ip_use                                 
--       ,case when c4.all_check_4_num = 0 then 0
--             else c4.in_3_check_num /c4.all_check_4_num  
--              end as propo_3_ip_visit 
--  from cdmpview.qspt_hzyykh_17_assess_1 a1
--  full join cdmpview.qspt_hzyykh_17_assess_2 a2
--    on a1.business_id = a2.business_id and a2.src_file_month = '${SRC_FILE_MONTH}'
--  full join cdmpview.qspt_hzyykh_17_assess_3 a3
--    on nvl(a1.business_id, a2.business_id) = a3.business_id and a3.src_file_month = '${SRC_FILE_MONTH}'
--  left join cdmpview.qspt_hzyykh_17_check_3 c3
--    on nvl(a1.business_id, a2.business_id) = c3.business_id and c3.src_file_month = '${SRC_FILE_MONTH}'
--  left join cdmpview.qspt_hzyykh_17_check_4 c4
--    on nvl(a1.business_id, a2.business_id) = c4.business_id and c4.src_file_month = '${SRC_FILE_MONTH}'
-- where a1.src_file_month = '${SRC_FILE_MONTH}';
 
${OUTPUT_RESULT} 
select a.statis_month
      ,a.business_id
      ,a.accu_add_revenue
      ,a.use_user_cnt
      ,a.in_order_user_cnt
      ,a.use_in_order_user_rate
      ,a.activ_visit
      ,a.tourist_rate
      ,case when nvl(c3.all_check_3_num, 0) = 0 then 0 
            else nvl(c3.in_3_check_num, 0)/c3.all_check_3_num  
             end as propo_3_ip_use                                 
      ,case when nvl(c4.all_check_4_num, 0) = 0 then 0
            else nvl(c4.in_3_check_num, 0)/c4.all_check_4_num  
             end as propo_3_ip_visit  
  from ( 
        select t.statis_month
              ,t.business_id
              ,sum(t.accu_add_revenue) as accu_add_revenue
              ,sum(t.use_user_cnt) as use_user_cnt
              ,sum(t.in_order_user_cnt) as in_order_user_cnt
              ,sum(t.use_in_order_user_rate) as use_in_order_user_rate
              ,sum(t.activ_visit) as activ_visit
              ,sum(t.tourist_rate) as tourist_rate 
          from ( 
                select a1.statis_month
                      ,a1.business_id
                      ,a1.accu_add_revenue
                      ,0 as use_user_cnt
                      ,0 as in_order_user_cnt
                      ,0 as use_in_order_user_rate
                      ,0 as activ_visit
                      ,0 as tourist_rate 
                  from cdmpview.qspt_hzyykh_17_assess_1 a1
                 where a1.src_file_month = '${SRC_FILE_MONTH}'
                 
                 union all
                 
                select a2.statis_month
                      ,a2.business_id
                      ,0 as accu_add_revenue
                      ,a2.use_user_cnt
                      ,a2.in_order_user_cnt
                      ,a2.use_in_order_user_rate
                      ,0 as activ_visit
                      ,0 as tourist_rate 
                  from cdmpview.qspt_hzyykh_17_assess_2 a2
                 where a2.src_file_month = '${SRC_FILE_MONTH}'
                 
                 union all
                 
                select a3.statis_month
                      ,a3.business_id
                      ,0 as accu_add_revenue
                      ,0 as use_user_cnt
                      ,0 as in_order_user_cnt
                      ,0 as use_in_order_user_rate
                      ,a3.activ_visit
                      ,a3.tourist_rate 
                  from cdmpview.qspt_hzyykh_17_assess_3 a3
                 where a3.src_file_month = '${SRC_FILE_MONTH}'
              ) t 
        group by t.statis_month, t.business_id 
       ) a            
  left join cdmpview.qspt_hzyykh_17_check_3 c3
    on a.business_id = c3.business_id and c3.src_file_month = '${SRC_FILE_MONTH}'
  left join cdmpview.qspt_hzyykh_17_check_4 c4
    on a.business_id = c4.business_id and c4.src_file_month = '${SRC_FILE_MONTH}'
 
