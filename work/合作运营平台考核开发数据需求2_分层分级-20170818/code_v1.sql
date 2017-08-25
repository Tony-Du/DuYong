

计算包括下列字段，类似之前你做的考核计算规则：
新增收入（从每年4月份开始计算，到本季度截至日期）
使用/在定 （3个月）
月累计访问（3个月）
活跃度（3个月）

维度是cpid（输出含公司名称），合作运营部按季度提供维表。

内容供应商
content provider



--累计新增收入
with busi_sub_busi as (
select a.business_id
      ,a.sub_busi_id
  from rptdata.dim_sub_busi a 
  join (select distinct business_id from ${DIM_BUSI_ID_TBL}) b
    on a.business_id = b.business_id
 group by a.business_id
         ,a.sub_busi_id 
)
insert overwrite table qushupingtai.qspt_hzyykh_17_assess_1 partition (src_file_month = '${SRC_FILE_MONTH}')
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
           and nvl(a.boss_repeat_order_flag,'') <> 'Y'
           and nvl(a.boss_last_success_bill_flag,'') <> 'N'
         group by b.business_id
         
         union all 
         
        select ar.business_id
              ,ar.add_revenue as accu_add_revenue     
          from qushupingtai.qspt_hzyykh_201704_05_add_revenue ar  
          join (select distinct business_id from ${DIM_BUSI_ID_TBL}) b
            on ar.business_id = b.business_id
         group by ar.business_id
                 ,ar.add_revenue
       ) t
 group by t.business_id;
 
 
 
 
--使用用户数 
with busi_sub_busi as (
select a.business_id
      ,a.sub_busi_id
  from rptdata.dim_sub_busi a
  join (select distinct business_id from ${DIM_BUSI_ID_TBL}) b 
    on a.business_id = b.business_id
 group by a.business_id
         ,a.sub_busi_id 
)
insert overwrite table qushupingtai.qspt_hzyykh_17_assess_2_u partition (src_file_month = '${SRC_FILE_MONTH}')
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
  join qushupingtai.qspt_hzyykh_17_check_1_2 b
    on a.usernum_id = b.usernum_id and a.business_id = b.business_id and b.src_file_month = '${SRC_FILE_MONTH}'
 where a.dept = '合作运营部' 
   and b.flow_kb >= ${FLOW_THRESHOLD_KB}
   and b.duration_sec >= ${DURATION_THRESHOLD_SEC} 
 group by a.business_id;
 
 
 
--在订用户数
with busi_sub_busi as (
select a.business_id
      ,a.sub_busi_id
  from rptdata.dim_sub_busi a
  join (select distinct business_id from ${DIM_BUSI_ID_TBL}) b 
    on a.business_id = b.business_id
 group by a.business_id
         ,a.sub_busi_id 
)
insert overwrite table qushupingtai.qspt_hzyykh_17_assess_2_order partition (src_file_month = '${SRC_FILE_MONTH}')      
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
 
 
--使用用户数/在订用户数 
insert overwrite TABLE qushupingtai.qspt_hzyykh_17_assess_2 partition (src_file_month = '${SRC_FILE_MONTH}')
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
          from qushupingtai.qspt_hzyykh_17_assess_2_u u 
         where u.src_file_month = '${SRC_FILE_MONTH}'
         
         union all 
         
        select o.statis_month
              ,o.business_id
              ,0 as use_user_cnt
              ,o.in_order_user_cnt
          from qushupingtai.qspt_hzyykh_17_assess_2_order o
         where o.src_file_month = '${SRC_FILE_MONTH}' 
       ) t  
group by t.statis_month, t.business_id



--访问用户数 & 注册访问用户数
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
insert overwrite table qushupingtai.qspt_hzyykh_17_assess_3_m partition (src_file_month = '${SRC_FILE_MONTH}')
select substr('${MONTH_START_DAY}', 1, 6) as statis_month
      ,t.business_id
      ,sum(t.login_visit_user_cnt) as login_visit_user_cnt 
      ,sum(t.visit_user_cnt) as visit_user_cnt 
  from ( 
        --select a.business_id
        --      ,case when a.user_type_id = '1' then count(distinct a.user_key) else 0 end as login_visit_user_cnt
        --      ,0 as visit_user_cnt 
        --  from assess_3_detail a         
        -- where a.dept = '合作运营部'
        -- group by a.business_id
        --         ,a.user_type_id
        --  
        -- union all
         
         select a.business_id
              ,0 as login_visit_user_cnt
              ,count(distinct a.user_key) visit_user_cnt 
          from assess_3_detail a         
         where a.dept = '合作运营部'
         group by a.business_id         
       ) t
 group by t.business_id ;
 
 
-- 活跃度，游客占比 
insert overwrite TABLE qushupingtai.qspt_hzyykh_17_assess_3 partition (src_file_month = '${SRC_FILE_MONTH}')
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
          from qushupingtai.qspt_hzyykh_17_assess_3_d d
         where d.src_file_month = '${SRC_FILE_MONTH}'

         union all 
         
        select m.statis_month
              ,m.business_id
              ,0 as day_avg_visit_user_cnt
              ,m.login_visit_user_cnt
              ,m.visit_user_cnt
          from qushupingtai.qspt_hzyykh_17_assess_3_m m 
         where m.src_file_month = '${SRC_FILE_MONTH}'
       ) t
 group by t.statis_month, t.business_id
 
 
 
