with busi_sub_busi as (
select a.business_id
      ,a.sub_busi_id
  from rptdata.dim_sub_busi a
  join ${DIM_BUSI_ID_TBL} b
    on a.business_id = b.business_id
 group by a.business_id
         ,a.sub_busi_id 
)
insert overwrite table cdmpview.qspt_hzyykh_05_check_1_2 
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
  join ${DIM_BUSI_ID_TBL} b 
    on a.business_id = b.business_id
 group by a.business_id
         ,a.sub_busi_id 
)
insert overwrite table cdmpview.qspt_hzyykh_05_check_3 
select substr('${MONTH_START_DAY}',1,6) as statis_month
      ,a.business_id
      ,sum(case when a.rn <= '${NUM_TOP_IP}' then a.check_3_u_num else 0 end) as in_3_check_num --这里还没有使用该参数
      ,sum(a.check_3_u_num) as all_check_3_num
  from (
        select t.business_id
              ,t.use_source_ip
              ,sum(t.check_3_u_num) as check_3_u_num
              ,row_number() over (partition by t.business_id order by sum(t.check_3_u_num) desc) rn
          from (     
                select fud.src_file_day
                      ,b.business_id
                      ,fud.use_source_ip   
                      ,count(distinct fud.usernum_id) as check_3_u_num 
                  from rptdata.fact_use_detail fud
                  join busi_sub_busi b
                    on fud.sub_busi_id = b.sub_busi_id 
                 where fud.src_file_day >= '${MONTH_START_DAY}' 
                   and fud.src_file_day <= '${MONTH_END_DAY}' 
                   and fud.user_type_id = '1' 
                 group by fud.src_file_day
                         ,b.business_id
                         ,fud.use_source_ip
               ) t               
        group by t.business_id
                ,t.use_source_ip         
       ) a
group by a.business_id;


with busi_sub_busi as (
select a.business_id
      ,a.sub_busi_id
  from rptdata.dim_sub_busi a
  join ${DIM_BUSI_ID_TBL} b
    on a.business_id = b.business_id
 group by a.business_id
         ,a.sub_busi_id 
)
insert overwrite table cdmpview.qspt_hzyykh_05_check_4 
select substr('${MONTH_START_DAY}',1,6) as statis_month
      ,a.business_id
      ,sum(case when a.rn <= '${NUM_TOP_IP}' then a.check_4_num else 0 end) as in_3_check_num --这里还没有使用该参数
      ,sum(a.check_4_num) as all_check_4_num
  from (
        select t.business_id
              ,t.client_ip
              ,sum(t.check_4_num) as check_4_num
              ,row_number() over (partition by t.business_id order by sum(t.check_4_num) desc) rn
          from (     
                select fuvh.src_file_day
                      ,b.business_id
                      ,fuvh.client_ip                        
                      ,count(distinct fuvh.user_key) as check_4_num   
                  from rptdata.fact_user_visit_hourly fuvh
                  join busi_sub_busi b
                    on fuvh.sub_busi_id = b.sub_busi_id 
                 where fuvh.src_file_day >= '${MONTH_START_DAY}' 
                   and fuvh.src_file_day <= '${MONTH_END_DAY}'
                 group by fuvh.src_file_day
                         ,b.business_id
                         ,fuvh.client_ip
               ) t               
        group by t.business_id
                ,t.client_ip         
       ) a
group by a.business_id;



with busi_sub_busi as (
select a.business_id
      ,a.sub_busi_id
  from rptdata.dim_sub_busi a
  join ${DIM_BUSI_ID_TBL} b
    on a.business_id = b.business_id
 group by a.business_id
         ,a.sub_busi_id 
)
insert overwrite table cdmpview.qspt_hzyykh_05_assess_1
select substr('${MONTH_END_DAY}', 1, 6) as statis_month
      ,b.business_id
      ,sum(a.payment_amt) as accu_add_revenue
  from rptdata.fact_order_item_detail a
  join busi_sub_busi b
    on a.sub_business_id = b.sub_busi_id
 where case when substr('${MONTH_END_DAY}', 5, 2) < '04' 
            then (a.src_file_day >= concat(substr(add_months(concat_ws('-', substr('${MONTH_END_DAY}', 1, 4), substr('${MONTH_END_DAY}', 5, 2), '01'), -12), 1, 4), '0401') 
                  and a.src_file_day <= '${MONTH_END_DAY}'
                 ) 
            else (a.src_file_day >= concat(substr('${MONTH_END_DAY}', 1, 4), '0401') and 
                  a.src_file_day <= '${MONTH_END_DAY}'
                 ) 
             end 
   and a.order_status in (5,9)
 group by b.business_id;
 


with busi_sub_busi as (
select a.business_id
      ,a.sub_busi_id
  from rptdata.dim_sub_busi a
  join ${DIM_BUSI_ID_TBL} b 
    on a.business_id = b.business_id
 group by a.business_id
         ,a.sub_busi_id 
)
insert overwrite table cdmpview.qspt_hzyykh_05_Assess_2_u 
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
  join cdmpview.qspt_hzyykh_05_check_1_2 b
    on a.usernum_id = b.usernum_id and a.business_id = b.business_id
 where a.dept = '合作运营部' 
   and b.flow_kb >= ${FLOW_THRESHOLD_KB}
   and b.duration_sec >= ${DURATION_THRESHOLD_SEC} 
 group by a.business_id;


with busi_sub_busi as (
select a.business_id
      ,a.sub_busi_id
  from rptdata.dim_sub_busi a
  join ${DIM_BUSI_ID_TBL} b 
    on a.business_id = b.business_id
 group by a.business_id
         ,a.sub_busi_id 
)
insert overwrite table cdmpview.qspt_hzyykh_05_Assess_2_order       
select substr('${MONTH_START_DAY}', 1, 6) as statis_month
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
         where a.snapshot_day >= '${MONTH_START_DAY}' and a.snapshot_day <= '${MONTH_END_DAY}' 
           and c.chrgprod_price > 0 
         group by b.business_id,
                  a.order_user_id
       ) t
 group by t.business_id;


insert overwrite TABLE cdmpview.qspt_hzyykh_05_Assess_2 
SELECT nvl(u.statis_month, o.statis_month ) statis_month
      ,nvl(u.business_id, o.business_id) business_id
      ,nvl(u.use_user_cnt, 0) use_user_cnt
      ,nvl(o.in_order_user_cnt, 0) in_order_user_cnt
      ,case when nvl(o.in_order_user_cnt, 0) = 0 then 0 else nvl(u.use_user_cnt, 0)/o.in_order_user_cnt end as use_in_order_user_rate
  FROM cdmpview.tmp_dy_05_Assess_2_u u 
  FULL JOIN cdmpview.tmp_dy_05_Assess_2_Order o
    ON u.business_id = o.business_id;


with busi_sub_busi as (
select a.business_id
      ,a.sub_busi_id
  from rptdata.dim_sub_busi a
  join ${DIM_BUSI_ID_TBL} b
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
insert overwrite table cdmpview.qspt_hzyykh_05_Assess_3_d 
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
                 ,a.user_type_id
       ) t
 group by t.business_id;


with busi_sub_busi as (
select a.business_id
      ,a.sub_busi_id
  from rptdata.dim_sub_busi a
  join ${DIM_BUSI_ID_TBL} b
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
insert overwrite table cdmpview.qspt_hzyykh_05_Assess_3_m 
select substr('${MONTH_START_DAY}', 1, 6) as statis_month
      ,t.business_id
      ,sum(t.login_visit_user_cnt) as login_visit_user_cnt 
      ,sum(t.visit_user_cnt) as visit_user_cnt 
  from (    
        select a.business_id
              ,case when a.user_type_id = '1' then count(distinct a.user_key) else 0 end as login_visit_user_cnt
              ,count(distinct a.user_key) visit_user_cnt 
          from assess_3_detail a         
         where a.dept = '合作运营部'
         group by a.business_id
                 ,a.user_type_id
       ) t
 group by t.business_id ;
 
 
insert overwrite TABLE cdmpview.qspt_hzyykh_05_Assess_3
select nvl(d.statis_month, m.statis_month) as statis_month
      ,nvl(d.business_id, m.business_id) as business_id
      ,nvl(d.day_avg_visit_user_cnt, 0) as day_avg_visit_user_cnt
      ,nvl(m.login_visit_user_cnt, 0) as login_visit_user_cnt   
      ,nvl(m.visit_user_cnt, 0) as visit_user_cnt
      ,case when nvl(m.visit_user_cnt, 0) = 0 then 0 
            else nvl(d.day_avg_visit_user_cnt, 0)/m.visit_user_cnt end as activ_visit 
      ,case when nvl(m.visit_user_cnt, 0) = 0 then 0 
            else 1 - (nvl(m.login_visit_user_cnt, 0)/m.visit_user_cnt) end as tourist_rate 
  from cdmpview.qspt_hzyykh_05_Assess_3_d d
  full join cdmpview.qspt_hzyykh_05_Assess_3_m m 
    on d.business_id = m.business_id;
    

${OUTPUT_RESULT}
select  a1.statis_month
       ,nvl(a1.business_id, a2.business_id) as business_id
       ,nvl(a1.accu_add_revenue, 0) as accu_add_revenue             
       ,nvl(a2.use_in_order_user_rate, 0) as use_in_order_user_rate 
       ,nvl(a3.activ_visit, 0) as activ_visit                       
       ,nvl(a3.tourist_rate, 0) as tourist_rate                     
       ,case when c3.all_check_3_num = 0 then 0 
             else c3.in_3_check_num/c3.all_check_3_num  
              end as propo_3_ip_use                                 
       ,case when c4.all_check_4_num =0 then 0
             else c4.in_3_check_num /c4.all_check_4_num  
              end as propo_3_ip_visit                                                
  from cdmpview.qspt_hzyykh_05_Assess_1 a1
  full join cdmpview.qspt_hzyykh_05_Assess_2 a2
    on a1.business_id = a2.business_id  
  full join cdmpview.qspt_hzyykh_05_Assess_3 a3
    on nvl(a1.business_id, a2.business_id) = a3.business_id  
  left join cdmpview.qspt_hzyykh_05_check_3 c3
    on nvl(a1.business_id, a2.business_id) = c3.business_id
  left join cdmpview.qspt_hzyykh_05_check_4 c4
    on nvl(a1.business_id, a2.business_id) = c4.business_id ;
