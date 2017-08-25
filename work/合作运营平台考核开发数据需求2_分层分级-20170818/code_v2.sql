 select company_id
       ,company_name
       ,busi_type
       ,business_id
       ,business_name
       ,sub_busi_id
       ,sub_busi_name
       ,program_formal_time
       ,partner_formal_time               
   from ${DIM_FCFJ_TBL} a 
  group by company_id
          ,company_name
          ,busi_type
          ,business_id
          ,business_name
          ,sub_busi_id
          ,sub_busi_name
          ,program_formal_time
          ,partner_formal_time
------------------------------------王玮提供的维表----
 
 
 
 
--累计新增收入 

with temp_dim_fcfj_tbl as (  --分层分级维表
select company_id, company_name, sub_busi_id              
  from ${DIM_FCFJ_TBL} a 
 group by company_id, company_name, sub_busi_id   
)
insert overwrite table qushupingtai.qspt_hzyykh_fcfj_accu_add_revenue partition (src_file_month = '${SRC_FILE_MONTH}') --201701 201704 201707 201710
select b.company_id
      ,sum(t.accu_add_revenue) as accu_add_revenue
  from (
        select a.sub_business_id as sub_busi_id
              ,sum(a.payment_amt) as accu_add_revenue
          from rptdata.fact_order_item_detail a
         where a.src_file_day >= '20170601' and a.src_file_day <= '${MONTH_END_DAY}' --20170331, 20170630, 20170930, 20171230
           and a.order_status in (5,9)
           and nvl(a.boss_repeat_order_flag,'') <> 'Y'
           and nvl(a.boss_last_success_bill_flag,'') <> 'N'
         group by a.sub_business_id

         union all

        select a.sub_busi_id 
              ,a.add_revenue as accu_add_revenue
          from qushupingtai.qspt_hzyykh_fcfj_201704_05_add_revenue a
         where a.statis_month between '201704' and '201705'
         group by a.sub_busi_id, a.add_revenue 
       ) t
   join temp_dim_fcfj_tbl b                        --分层分级维表
     on t.sub_busi_id = b.sub_busi_id
  group by b.company_id ;
  

===========================================================================================================================


  
--使用用户数

with temp_dim_fcfj_tbl as (  --分层分级维表
select company_id, business_id, sub_busi_id              
  from ${DIM_FCFJ_TBL} a 
 group by company_id, business_id, sub_busi_id   
),
temp_use_user_tbl as (
select case when tp.term_prod_name in ('和视频OPENAPI', '和视频SDK') then nvl(dc.dept_name,'9999') 
            else nvl(tp.dept_name, nvl(bu.dept, nvl(dc.dept_name, '9999'))) end as dept 
      ,b.company_id
      ,a.usernum_id
  from rptdata.fact_use_detail a  
  join temp_dim_fcfj_tbl b
    on a.sub_busi_id = b.sub_busi_id     
  left join rptdata.dim_dept_term_prod tp
    on a.termprod_id = tp.term_prod_id    
  left join cdmpview.tmp_wsj_0606_dim_busi_new bu --有待确认
    on b.business_id = bu.business_id    
  left join rptdata.dim_dept_chn dc
    on a.channel_id = dc.chn_id    
 where a.src_file_day >= '${MONTH_START_DAY}'  --20170701   
   and a.src_file_day <= '${MONTH_END_DAY}'    --20170731
   and a.user_type_id = '1' 
 group by case when tp.term_prod_name in ('和视频OPENAPI', '和视频SDK') then nvl(dc.dept_name,'9999') 
               else nvl(tp.dept_name, nvl(bu.dept, nvl(dc.dept_name, '9999'))) end
         ,b.company_id
         ,a.usernum_id 
)
insert overwrite table qushupingtai.qspt_hzyykh_fcfj_use_user partition (src_file_month = '${SRC_FILE_MONTH}')
select tt.company_id 
      ,count(tt.usernum_id) as use_user_cnt
  from (      
        select t.company_id
              ,t.usernum_id
          from temp_use_user_tbl t         
         where t.dept = '合作运营部'   --限制部门
         group by t.company_id, t.usernum_id
       ) tt  
 group by tt.company_id ;
 

  
--在订用户数 
with temp_dim_fcfj_tbl as (  --分层分级维表
select company_id, business_id, sub_busi_id              
  from ${DIM_FCFJ_TBL} a 
 group by company_id, business_id, sub_busi_id   
),
temp_in_order_user_tbl as (
select a.sub_business_id as sub_busi_id
      ,a.order_user_id
  from rptdata.fact_order_daily_snapshot a
  join rptdata.dim_charge_product c
    on a.product_id = c.chrgprod_id   
 where a.snapshot_day = '${MONTH_END_DAY}'  
   and c.chrgprod_price > 0                   --付费
 group by a.sub_business_id, a.order_user_id  
)
insert overwrite table qushupingtai.qspt_hzyykh_fcfj_in_order_user partition (src_file_month = '${SRC_FILE_MONTH}') 
select b.company_id
      ,count(t.order_user_id) as in_order_user_cnt
  from temp_in_order_user_tbl t 
  join temp_dim_fcfj_tbl b  
    on t.sub_busi_id = b.sub_busi_id   
 group by b.company_id ;  
  

  
--使用用户数/在订用户数   
insert overwrite TABLE qushupingtai.qspt_hzyykh_fcfj_use_in_order_user_rate partition (src_file_month = '${SRC_FILE_MONTH}') 
select company_id
      ,sum(use_user_cnt) as use_user_cnt
      ,sum(in_order_user_cnt) as in_order_user_cnt
      ,case when in_order_user_cnt = 0 then 0 else sum(use_user_cnt)/sum(in_order_user_cnt) end as use_in_order_user_rate
  from (  
        select company_id
              ,use_user_cnt
              ,0 as in_order_user_cnt
          from qushupingtai.qspt_hzyykh_fcfj_in_order_user a 
         where a.src_file_month = '${SRC_FILE_MONTH}'
          
         union all
         
        select company_id
              ,0 as use_user_cnt
              ,in_order_user_cnt
          from qushupingtai.qspt_hzyykh_fcfj_in_order_user a 
         where a.src_file_month = '${SRC_FILE_MONTH}'  
       ) t 
 group by company_id;

 


==========================================================================================================================================
 
 
--月剔重访问用户数
with temp_dim_fcfj_tbl as (  --分层分级维表
select company_id, business_id, sub_busi_id              
  from ${DIM_FCFJ_TBL} a 
 group by company_id, business_id, sub_busi_id   
),
temp_mon_user_visit as (     --一个月的 访问数据 
select case when tp.term_prod_name in ('和视频OPENAPI', '和视频SDK') then nvl(dc.dept_name,'9999') 
            else nvl(tp.dept_name, nvl(bu.dept, nvl(dc.dept_name, '9999'))) end as dept 
      ,b.company_id
      ,a.user_key
  from rptdata.fact_user_visit_hourly a   
  join temp_dim_fcfj_tbl b 
    on a.sub_busi_id = b.sub_busi_id   
  left join rptdata.dim_dept_term_prod tp
    on a.term_prod_id = tp.term_prod_id         
  left join cdmpview.tmp_wsj_0606_dim_busi_new bu 
    on b.business_id = bu.business_id         
  left join rptdata.dim_dept_chn dc
    on a.chn_id = dc.chn_id     
 where a.src_file_day >= '${MONTH_START_DAY}'    --20170701 
   and a.src_file_day <= '${MONTH_END_DAY}'      --20170731 
 group by case when tp.term_prod_name in ('和视频OPENAPI', '和视频SDK') then nvl(dc.dept_name,'9999') 
            else nvl(tp.dept_name, nvl(bu.dept, nvl(dc.dept_name, '9999'))) end
         ,b.company_id
         ,a.user_key
)
insert overwrite TABLE qushupingtai.qspt_hzyykh_fcfj_mon_visit_user partition (src_file_month = '${SRC_FILE_MONTH}') 
select t.company_id
      ,count(distinct t.user_key) as mon_visit_user_cnt
  from temp_mon_user_visit t  
 where t.dept = '合作运营部' 
 group by company_id;




  
==========================================================================================================================================  



--日均访问用户数
with temp_dim_fcfj_tbl as (  --分层分级维表
select company_id, business_id, sub_busi_id              
  from ${DIM_FCFJ_TBL} a 
 group by company_id, business_id, sub_busi_id   
),
temp_mon_user_visit as (     --一个月的 访问数据 
select case when tp.term_prod_name in ('和视频OPENAPI', '和视频SDK') then nvl(dc.dept_name,'9999') 
            else nvl(tp.dept_name, nvl(bu.dept, nvl(dc.dept_name, '9999'))) end as dept 
      ,a.src_file_day
      ,b.company_id
      ,a.user_key
  from rptdata.fact_user_visit_hourly a   
  join temp_dim_fcfj_tbl b 
    on a.sub_busi_id = b.sub_busi_id       
  left join rptdata.dim_dept_term_prod tp
    on a.term_prod_id = tp.term_prod_id             
  left join cdmpview.tmp_wsj_0606_dim_busi_new bu --有待确认
    on b.business_id = bu.business_id     
  left join rptdata.dim_dept_chn dc
    on a.chn_id = dc.chn_id          
 where a.src_file_day >= '${MONTH_START_DAY}'    --20170701 
   and a.src_file_day <= '${MONTH_END_DAY}'      --20170731 
 group by case when tp.term_prod_name in ('和视频OPENAPI', '和视频SDK') then nvl(dc.dept_name,'9999') 
            else nvl(tp.dept_name, nvl(bu.dept, nvl(dc.dept_name, '9999'))) end
         ,a.src_file_day
         ,a.sub_busi_id
         ,a.user_key
)
insert overwrite table qushupingtai.qspt_hzyykh_fcfj_day_avg_visit_user partition (src_file_month = '${SRC_FILE_MONTH}') 
select tt.company_id
      ,sum(visit_user_cnt)/cast(substr('${MONTH_END_DAY}',7,2) as int) as day_avg_visit_user_cnt 
  from (
        select t.src_file_day
              ,t.company_id
              ,count(distinct t.user_key) as visit_user_cnt
          from temp_mon_user_visit t
         where t.dept = '合作运营部'  
         group by t.src_file_day, b.company_id
       ) tt 
 group by tt.company_id
 
 
-- 活跃度
insert overwrite table qushupingtai.qspt_hzyykh_fcfj_activ_visit partition (src_file_month = '${SRC_FILE_MONTH}')  
select company_id
      ,sum(day_avg_visit_user_cnt) as day_avg_visit_user_cnt
      ,sum(mon_visit_user_cnt) as mon_visit_user_cnt
      ,case when sum(mon_visit_user_cnt) = 0 then 0 
            else sum(t.day_avg_visit_user_cnt)/sum(t.mon_visit_user_cnt) end as activ_visit
  from ( 
        select company_id
              ,day_avg_visit_user_cnt
              ,0 as mon_visit_user_cnt
          from qushupingtai.qspt_hzyykh_fcfj_day_avg_visit_user_cnt d
         where d.src_file_month = '${SRC_FILE_MONTH}'
         
         union all
         
        select company_id
              ,0 as day_avg_visit_user_cnt
              ,mon_visit_user_cnt
          from qushupingtai.qspt_hzyykh_fcfj_mon_visit_user_cnt m
         where m.src_file_month = '${SRC_FILE_MONTH}'
       ) t
 group by company_id
 
==================================================================================================

select company_id
      ,sum(accu_add_revenue) as accu_add_revenue
      ,sum(use_in_order_user_rate)/3 as use_in_order_user_rate --季度内平均每月 使用用户数/在订用户数 
      ,sum(visit_user_cnt) as mon_accu_visit_user_cnt          --一个季度内 月累计访问用户数 
      ,sum(activ_visit)/3 as activ_visit                       --一个季度内 月活跃度
  from ( 
        select company_id
              ,accu_add_revenue
              ,0 as use_in_order_user_rate
              ,0 as mon_visit_user_cnt
              ,0 as activ_visit
          from qushupingtai.qspt_hzyykh_fcfj_accu_add_revenue a
         where a.src_file_month = '201709'

         union all
         
        select company_id
              ,0 as accu_add_revenue
              ,use_in_order_user_rate
              ,0 as mon_visit_user_cnt
              ,0 as activ_visit
          from qushupingtai.qspt_hzyykh_fcfj_use_in_order_user_rate a 
         where a.src_file_month in ('201707','201708','201709')
         
         union all
        
        select company_id
              ,0 as accu_add_revenue
              ,0 as use_in_order_user_rate
              ,mon_visit_user_cnt
              ,0 as activ_visit
           from qushupingtai.qspt_hzyykh_fcfj_mon_accu_visit_user a 
          where a.src_file_month in ('201707', '201708', '201709')    

          union all
          
        select company_id
              ,0 as accu_add_revenue
              ,0 as use_in_order_user_rate
              ,0 as mon_visit_user_cnt
              ,activ_visit
           from qushupingtai.qspt_hzyykh_fcfj_activ_visit a 
          where a.src_file_month in ('201707', '201708', '201709')             
       ) t
 group by company_id;
 
 
 


 
 
 

--字段名称    指标口径    累计新增收入

--自2017年4月1日起至考核当月累计新增收入
--使用用户数与在订付费用户比例  分子为考核月使用合作伙伴业务的注册用户数，分母为考核月月末合作伙伴业务付费在订用户数

--访问活跃度   日均访问用户数/月剔重访问用户数

--月累计访问=a月剔重+b月剔重+c月剔重
 
 