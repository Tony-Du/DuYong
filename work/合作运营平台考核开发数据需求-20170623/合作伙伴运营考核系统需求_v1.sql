
--把 枚举的 business_id 做成一个维表



'${DIM_BUSI_ID_TBL}'

('B1000100000',
'B1000100020',
'B1000100034',
'B1000101400',
'B1000101401',
'B1000101500',
'B2000100001',
'B2000100021',
'B2000100022',
'B2000100031',
'B2000100036',
'B2000100037',
'B2000100038',
'B2000100039',
'B2000100040',
'B2000100041',
'B2000100042',
'B2000100045',
'B2000101701',
'B2000101702',
'B2000101703',
'B2000101704',
'B2000101705',
'B2000101800',
'B2000101801',
'B2000101802',
'B2000101803',
'B2000101804',
'B2000102101',
'B2000102102',
'B2000102301',
'B2000102401',
'B2000102402',
'B2000102801',
'B2000102901',
'B2000102902',
'B2000103301',
'B2000103701',
'B2000103702',
'B2000103801',
'B2000104001',
'B2000104402',
'BA000104501',
'BA000104502',
'BA000104503',
'BA000104504',
'BA000104505',
'BA000104506',
'BA000104601',
'BA000104602',
'BA000104603',
'BC000103501',
'BC000103503',
'BC000103504',
'BC000103505',
'BC000103506',
'BC000103507',
'BC000103508',
'BC000103509',
'BC000103510',
'BC000103601',
'BC000104604')

with busi_sub_busi as (
select a.business_id
      ,a.sub_busi_id
  from rptdata.dim_sub_busi a
  join ${DIM_BUSI_ID_TBL} b  -- 该表中的数据是手动可调的，用来限定 business_id 范围
    on a.business_id = b.business_id
 group by a.business_id
         ,a.sub_busi_id 
)


--测试所用
with busi_sub_busi as (
select a.business_id
      ,a.sub_busi_id
  from rptdata.dim_sub_busi a
 where a.business_id in (
                        'B1000100000',
                        'B1000100020',
                        'B1000100034',
                        'B1000101400',
                        'B1000101401',
                        'B1000101500',
                        'B2000100001',
                        'B2000100021',
                        'B2000100022',
                        'B2000100031',
                        'B2000100036',
                        'B2000100037',
                        'B2000100038',
                        'B2000100039',
                        'B2000100040',
                        'B2000100041',
                        'B2000100042',
                        'B2000100045',
                        'B2000101701',
                        'B2000101702',
                        'B2000101703',
                        'B2000101704',
                        'B2000101705',
                        'B2000101800',
                        'B2000101801',
                        'B2000101802',
                        'B2000101803',
                        'B2000101804',
                        'B2000102101',
                        'B2000102102',
                        'B2000102301',
                        'B2000102401',
                        'B2000102402',
                        'B2000102801',
                        'B2000102901',
                        'B2000102902',
                        'B2000103301',
                        'B2000103701',
                        'B2000103702',
                        'B2000103801',
                        'B2000104001',
                        'B2000104402',
                        'BA000104501',
                        'BA000104502',
                        'BA000104503',
                        'BA000104504',
                        'BA000104505',
                        'BA000104506',
                        'BA000104601',
                        'BA000104602',
                        'BA000104603',
                        'BC000103501',
                        'BC000103503',
                        'BC000103504',
                        'BC000103505',
                        'BC000103506',
                        'BC000103507',
                        'BC000103508',
                        'BC000103509',
                        'BC000103510',
                        'BC000103601',
                        'BC000104604'
                                    )
 group by a.business_id
         ,a.sub_busi_id 
)

--稽核1、2
--当月 该业务下 每个人 使用流量kb/使用时长s
insert overwrite table cdmpview.tmp_dy_05_check_1_2 
select substr('${MONTH_START_DAY}', 1, 6) statis_month  --201705
      ,b.business_id
      ,fud.usernum_id
      ,sum(fud.flow_kb) as flow_kb
      ,sum(fud.duration_sec) as duration_sec 
  from rptdata.fact_use_detail fud
  join busi_sub_busi b 
    on fud.sub_busi_id = b.sub_busi_id 
 where fud.src_file_day >= '${MONTH_START_DAY}' 
   and fud.src_file_day <= '${MONTH_END_DAY}' --20170501  20170531
   and fud.user_type_id = '1'  --使用用户、非游客
 group by b.business_id
         ,fud.usernum_id
         
                 
--稽核3   
insert overwrite table cdmpview.tmp_dy_05_check_3 
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
                select fud.src_file_day
                      ,b.business_id
                      ,fud.use_source_ip                        --用户源IP
                      ,count(distinct fud.usernum_id) as check_3_u_num   --每天、每个IP下 该业务的 使用用户数
                  from rptdata.fact_use_detail fud
                  join busi_sub_busi b
                    on fud.sub_busi_id = b.sub_busi_id 
                 where fud.src_file_day >= '${MONTH_START_DAY}' 
                   and fud.src_file_day <= '${MONTH_END_DAY}' --20170501  20170531
                   and fud.user_type_id = '1' 
                 group by fud.src_file_day
                         ,b.business_id
                         ,fud.use_source_ip
               ) t               
        group by t.business_id
                ,t.use_source_ip         
       ) a
group by a.business_id     


--稽核4   
insert overwrite table cdmpview.tmp_dy_05_check_4 
select substr('${MONTH_START_DAY}',1,6) as statis_month
      ,a.business_id
      ,sum(case when a.rn <= 3 then a.check_4_num else 0 end) as in_3_check_4_num
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
                   and fuvh.src_file_day <= '${MONTH_END_DAY}' --20170501  20170531
                 group by fuvh.src_file_day
                         ,b.business_id
                         ,fuvh.client_ip
               ) t               
        group by t.business_id
                ,t.client_ip         
       ) a
group by a.business_id  


--Assess_1
--累计新增收入:自2017年4月1日起至考核当月累计新增收入

insert overwrite table cdmpview.tmp_dy_05_assess_1_add_revenue partition (src_file_month = '${SRC_FILE_MONTH}')  
select t.statis_month
      ,t.business_id
      ,sum(t.accu_add_revenue) as accu_add_revenue --累计新增收入
  from (
        select '${SRC_FILE_MONTH}' as statis_month
              ,sb.business_id
              ,sum(a.payment_amt) as accu_add_revenue
          from rptdata.fact_order_item_detail a
          join rptdata.dim_sub_busi sb
            on a.sub_business_id = sb.sub_busi_id
         where substr(a.src_file_day, 1, 6) = '${SRC_FILE_MONTH}'  --当月
           and a.order_status in (5,9)
         group by sb.business_id
         
         union all
         
        select a.statis_month
              ,a.business_id
              ,a.accu_add_revenue
          from cdmpview.tmp_dy_05_assess_1_add_revenue a
         where (substr('${SRC_FILE_MONTH}', 5, 2) <> '04' and a.src_file_month = '${LAST_MONTH}') --当月不是4月份时， 取上月的累计新增收入
       ) t                   
 group by t.statis_month, t.business_id

         

-- Assess_2
--使用用户数与在订付费用户比例:
--分子为考核月使用合作伙伴业务的 注册用户数，分母为考核月月末合作伙伴业务 付费在订用户数

insert overwrite table cdmpview.tmp_dy_05_Assess_2_u 
select substr('${MONTH_START_DAY}', 1, 6) as statis_month
      ,a.business_id
      ,count(a.usernum_id) as use_user_cnt --当月 稽核后的 使用合作伙伴业务的 注册用户数
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
           and fud.src_file_day <= '${MONTH_END_DAY}' --20170501  20170531
           and fud.user_type_id = '1'
         group by case when tp.term_prod_name in ('和视频OPENAPI', '和视频SDK') then nvl(dc.dept_name,'9999') 
                       else nvl(tp.dept_name, nvl(bu.dept, nvl(dc.dept_name, '9999')))  end
                 ,b.business_id
                 ,fud.usernum_id         
       ) a
  join cdmpview.tmp_dy_05_check_1_2 b
    on a.usernum_id = b.usernum_id and a.business_id = b.business_id
 where a.dept = '合作运营部' 
   and b.flow_kb >= '${FLOW_THRESHOLD_KB}'      --0.3*1024 --使用流量阈值
   and b.duration_sec >= '${DURATION_THRESHOLD_SEC}' --1   --使用时长阈值
 group by a.business_id   

         
         
insert overwrite table cdmpview.tmp_dy_05_Assess_2_order       
select substr('${MONTH_END_DAY}', 1, 6) as statis_month
      ,t.business_id 
      ,count(t.usernum) as in_order_user_cnt
  from (
        select b.business_id
              ,a.usernum           --考核月月末 合作伙伴业务付费 在订用户号码
          from intdata.ugc_90104_monthorder_union a  --注意
          join rptdata.dim_charge_product c
            on a.product_id = c.chrgprod_id 
          join busi_sub_busi b
            on c.sub_busi_bdid = b.sub_busi_id 
         where a.src_file_day = '${MONTH_END_DAY}'    --20170531
           and a.order_status <> '4'
           and c.chrgprod_price > 0 
         group by b.business_id,
                  a.usernum
       ) t
 group by t.business_id 


insert overwrite TABLE cdmpview.tmp_dy_05_Assess_2 
SELECT nvl(u.statis_month, o.statis_month ) statis_month
      ,nvl(u.business_id, o.business_id) business_id
      ,nvl(u.use_user_cnt, 0) use_user_cnt
      ,nvl(o.in_order_user_cnt, 0) in_order_user_cnt
      ,case when nvl(o.in_order_user_cnt, 0) = 0 then 0 else nvl(u.use_user_cnt, 0)/o.in_order_user_cnt end as use_in_order_user_rate --使用用户数与在订付费用户比例
  FROM cdmpview.tmp_dy_05_Assess_2_u u 
  FULL JOIN cdmpview.tmp_dy_05_Assess_2_Order o
    ON u.business_id = o.business_id;

    
--Assess_3
--访问活跃度 日均访问用户数/月剔重访问用户数


with assess_3_detail as (
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
insert overwrite table cdmpview.tmp_dy_05_Assess_3_d 
select substr('${MONTH_START_DAY}', 1, 6) as statis_month
      ,t.business_id 
      ,sum(t.visit_user_cnt)/cast(substr('${MONTH_END_DAY}',7,2) as int) as day_avg_visit_user_cnt --当月 日均访问用户数
  from (
        select a.src_file_day
              ,a.business_id           
              ,count(distinct a.user_key) visit_user_cnt  --日 剔重 访问用户数
          from assess_3_detail a         
         where a.dept = '合作运营部'
         group by a.src_file_day
                 ,a.business_id
                 ,a.user_type_id
       ) t
 group by t.business_id    
         
                 
insert overwrite table cdmpview.tmp_dy_05_Assess_3_m 
select substr('${MONTH_START_DAY}', 1, 6) as statis_month
      ,t.business_id
      ,sum(t.login_visit_user_cnt) as login_visit_user_cnt --当月 剔重 注册访问用户数
      ,sum(t.visit_user_cnt) as visit_user_cnt             --当月 剔重 访问用户数
  from (    
        select a.business_id
              ,case when a.user_type_id = '1' then count(distinct a.user_key) else 0 end as login_visit_user_cnt --当月 剔重 注册访问用户数
              ,count(distinct a.user_key) visit_user_cnt  --当月 剔重 访问用户数
          from assess_3_detail a         
         where a.dept = '合作运营部'
         group by a.business_id
                 ,a.user_type_id
       ) t
 group by t.business_id    
         

--Assess_4
--节目观看次数均值  节目人均使用次数/当月上传节目数量
insert overwrite table cdmpview.tmp_dy_05_Assess_4 
select substr('${MONTH_START_DAY}', 1, 6) as statis_month
      ,t.business_id
      ,(t.assess_4_cnt/t.assess_4_user_num)/t.assess_4_pr_num as prog_watch_avg_cnt --节目观看次数均值
  from (
        select p.program_id as business_id   
              ,count(distinct a.usernum_id) as assess_4_user_num  --使用用户数
              ,count(1) as assess_4_cnt                     --使用次数
              ,count(distinct p.business_id) assess_4_pr_num --上传节目数量
          from rptdata.fact_use_detail a
          join cdmpview.tmp_wsj_Assess_4_program p
            on a.program_id = p.business_id
         where a.src_file_day >= '${MONTH_START_DAY}}' 
           and a.src_file_day <= '${MONTH_END_DAY}}'
         group by p.program_id 
       ) t

--Assess_5
--节目观看时长均值  节目人均使用时长/当月上传节目数量
insert overwrite table cdmpview.tmp_dy_05_Assess_5 
select substr('${MONTH_START_DAY}', 1, 6) as statis_month
      ,t.business_id
      ,(t.assess_5_duration_sec/t.assess_5_user_num)/t.assess_5_pr_num as prog_watch_avg_dur_sec
  from (
        select p.program_id as business_id  
              ,count(distinct a.usernum_id) as assess_5_user_num  --使用用户数
              ,sum(a.duration_sec) as assess_5_duration_sec   --使用时长s
              ,count(distinct p.business_id) as assess_5_pr_num --上传节目数量
          from rptdata.fact_use_detail a
          join cdmpview.tmp_wsj_Assess_4_program p
            on a.program_id = p.business_id
         where a.src_file_day >= '${MONTH_START_DAY}}' 
           and a.src_file_day <= '${MONTH_END_DAY}}'
         group by p.program_id 
       ) t


	   
--查询语句       
select  a.statis_month
       ,nvl(a.business_id, b.business_id) as business_id
       ,a.use_user_cnt           --使用用户数
       ,a.in_order_user_cnt      --在订付费用户数
       ,a.use_in_order_user_rate --使用用户数与在订付费用户比例      
       ,m.login_visit_user_cnt   --当月 剔重 注册访问用户数
       ,m.visit_user_cnt         --当月 剔重 访问用户数         
       ,case when c.all_check_3_num = 0 then 0 
             else c.in_3_check_num/c.all_check_3_num  --前3名占比
              end as propo_3_ip_use          
       ,case when d.all_check_4_num =0 then 0
             else d.in_4_check_num /d.all_check_4_num  --前3名占比  in_3_check_num
              end as propo_3_ip_visit           
       ,case when m.visit_user_cnt = 0 then 0
             else b.day_avg_visit_user_cnt/m.visit_user_cnt --访问活跃度：日均访问用户数/月剔重访问用户数
              end as activ_visit              
       ,case when m.visit_user_cnt = 0 then 0
             else 1-(m.login_visit_user_cnt/m.visit_user_cnt)
              end as tourist_rate             -- 游客占比
  from cdmpview.tmp_dy_05_Assess_2 a   
  full join cdmpview.tmp_dy_05_Assess_3_d b
    on a.business_id = b.business_id
  full join cdmpview.tmp_dy_05_Assess_3_m m
    on nvl(a.business_id, b.business_id) = m.business_id    
  left join cdmpview.tmp_dy_05_check_3 c
    on nvl(a.business_id, b.business_id) = c.business_id
  left join cdmpview.tmp_dy_05_check_4 d
    on nvl(a.business_id, b.business_id) = d.business_id 


==================================================================================
字段名称:                       指标口径:
累计新增收入                  自2017年4月1日起至考核当月累计新增收入
使用用户数与在订付费用户比例  分子为考核月使用合作伙伴业务的注册用户数，分母为考核月月末合作伙伴业务付费在订用户数
访问活跃度                       日均访问用户数/月剔重访问用户数
节目观看次数均值                节目人均使用次数/当月上传节目数量
节目观看时长均值                节目人均使用时长/当月上传节目数量


稽核参考指标:     被稽核指标:
人均使用流量      使用用户数（不含游客）
人均使用时长      使用用户数（不含游客）
使用用户IP集中度   使用用户数（不含游客）
访问用户IP集中度   访问活跃度
访问用户中游客占比   访问活跃度









        