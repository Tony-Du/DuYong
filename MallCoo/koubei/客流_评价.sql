
增量评价

-- 期间内 店铺 获得的 总的获得的星数

select PID
      ,mall_shop_id
      ,mall_shop_name
      ,shop_id
      ,shop_name            
      ,sum(comment_score) as comment_score
  from alipay_ddm_adm_ddm_ot_mall_shop_comment_dd
 where to_char(comment_time, 'yyyymmdd') between '${beg_date}' and '${end_date}'
 group by PID
         ,mall_shop_id
         ,mall_shop_name
         ,shop_id
         ,shop_name

@@{yyyyMMdd} 

评价详情mallcoo_koubei_mall_shop_comment_d

--增量评价值

-- 按 店铺、时间和星数 分组统计 各星数的个数
--每天 每个店铺 获得的星数 各是多少
select PID
      ,mall_shop_id
      ,mall_shop_name
      ,shop_id
      ,shop_name
      ,to_char(comment_time, 'yyyymmdd') as comment_time_p
      ,comment_score
      ,count(*) as comment_score_cnt
  from alipay_ddm_adm_ddm_ot_mall_shop_comment_dd
 where to_char(comment_time, 'yyyymmdd') between '${beg_date}' and '${end_date}'
 group by PID
         ,mall_shop_id
         ,mall_shop_name
         ,shop_id
         ,shop_name
         ,comment_score
         ,to_char(comment_time, 'yyyymmdd')

         
--期间内最近30条的评价

select PID
      ,mall_shop_id
      ,mall_shop_name
      ,shop_id
      ,shop_name      
      ,comment_time
      ,content
      ,comment_score
  from alipay_ddm_adm_ddm_ot_mall_shop_comment_dd 
 where dt between '${beg_date}' and '${end_date}'
 order by comment_time desc
 limit 30;
 


 
 
============================================================================ 

项目客流（按天）


--按mallid 、天 分组
select pid
      ,mall_shop_id
      ,mall_shop_name
      ,dt
      ,sum(user_cnt) as user_cnt
  from alipay_ddm_adm_ddm_ot_mall_hot_diagram_dd
 where dt between '${beg_date}' and '${end_date}'
 group by pid
         ,mall_shop_id
         ,mall_shop_name
         ,dt

         
         
         
         
select pid
      ,mall_shop_id
      ,mall_shop_name
      ,sum(user_cnt)/count(dt) as avg_user_cnt
  from (         
        select pid
              ,mall_shop_id
              ,mall_shop_name
              ,dt
              ,sum(user_cnt) as user_cnt
          from alipay_ddm_adm_ddm_ot_mall_hot_diagram_dd
         where dt between '${beg_date}' and '${end_date}'
         group by pid
                 ,mall_shop_id
                 ,mall_shop_name
                 ,dt
       ) a
group by pid
        ,mall_shop_id
        ,mall_shop_name

 


       
客流周度分布
--按 星期几 来分组


select pid
      ,mall_shop_id
      ,mall_shop_name
      ,case when coalesce(sum(monday_cnt),0) = 0 then 0 else sum(monday_user_cnt)/sum(monday_cnt) end as monday_avg_cnt
      ,case when coalesce(sum(tuesday_cnt),0) = 0 then 0 else sum(tuesday_user_cnt)/sum(tuesday_cnt) end as tuesday_avg_cnt
      ,case when coalesce(sum(wednesday_cnt),0) = 0 then 0 else sum(wednesday_user_cnt)/sum(wednesday_cnt) end as wednesday_avg_cnt
      ,case when coalesce(sum(thursday_cnt),0) = 0 then 0 else sum(thursday_user_cnt)/sum(thursday_cnt) end as thursday_avg_cnt      
      ,case when coalesce(sum(friday_cnt),0) = 0 then 0 else sum(friday_user_cnt)/sum(friday_cnt) end as friday_avg_cnt      
      ,case when coalesce(sum(saturday_cnt),0) = 0 then 0 else sum(saturday_user_cnt)/sum(saturday_cnt) end as saturday_avg_cnt
      ,case when coalesce(sum(sunday_cnt),0) = 0 then 0 else sum(sunday_user_cnt)/sum(sunday_cnt) end as sunday_avg_cnt
  from (
        select pid
              ,mall_shop_id
              ,mall_shop_name              
              ,sum(case when week_dt = '周一' then 1 else 0 end) as monday_cnt
              ,sum(case when week_dt = '周二' then 1 else 0 end) as tuesday_cnt
              ,sum(case when week_dt = '周三' then 1 else 0 end) as wednesday_cnt
              ,sum(case when week_dt = '周四' then 1 else 0 end) as thursday_cnt
              ,sum(case when week_dt = '周五' then 1 else 0 end) as friday_cnt
              ,sum(case when week_dt = '周六' then 1 else 0 end) as saturday_cnt
              ,sum(case when week_dt = '周日' then 1 else 0 end) as sunday_cnt              
              ,sum(case when week_dt = '周一' then user_cnt else 0 end) as monday_user_cnt
              ,sum(case when week_dt = '周二' then user_cnt else 0 end) as tuesday_user_cnt
              ,sum(case when week_dt = '周三' then user_cnt else 0 end) as wednesday_user_cnt
              ,sum(case when week_dt = '周四' then user_cnt else 0 end) as thursday_user_cnt
              ,sum(case when week_dt = '周五' then user_cnt else 0 end) as friday_user_cnt
              ,sum(case when week_dt = '周六' then user_cnt else 0 end) as saturday_user_cnt
              ,sum(case when week_dt = '周日' then user_cnt else 0 end) as sunday_user_cnt
          from (
                select pid
                      ,mall_shop_id
                      ,mall_shop_name
                      ,dt                      
                      ,case when WEEKDAY(to_date(dt,'yyyymmdd')) = 0 then '周一' 
                            when WEEKDAY(to_date(dt,'yyyymmdd')) = 1 then '周二' 
                            when WEEKDAY(to_date(dt,'yyyymmdd')) = 2 then '周三' 
                            when WEEKDAY(to_date(dt,'yyyymmdd')) = 3 then '周四' 
                            when WEEKDAY(to_date(dt,'yyyymmdd')) = 4 then '周五' 
                            when WEEKDAY(to_date(dt,'yyyymmdd')) = 5 then '周六' 
                            when WEEKDAY(to_date(dt,'yyyymmdd')) = 6 then '周日' end as week_dt
                      ,user_cnt
                  from (
                         select pid
                               ,mall_shop_id
                               ,mall_shop_name
                               ,dt
                               ,sum(user_cnt) as user_cnt
                           from alipay_ddm_adm_ddm_ot_mall_hot_diagram_dd
                          where dt between '${beg_date}' and '${end_date}'
                          group by pid
                                  ,mall_shop_id
                                  ,mall_shop_name
                                  ,dt                       
                       ) t
               ) a
         group by pid
                 ,mall_shop_id
                 ,mall_shop_name
       ) b
 group by pid
      ,mall_shop_id
      ,mall_shop_name

      
      
-- 工作日、周末 日均客流
select pid
      ,mall_shop_id
      ,mall_shop_name
      ,case when sum(weekday_cnt) = 0 then 0 else sum(weekday_user_cnt)/sum(weekday_cnt) end as weekday_avg_cnt
      ,case when sum(weekend_cnt) = 0 then 0 else sum(weekend_user_cnt)/sum(weekend_cnt) end as weekend_avg_cnt
  from (
        select pid
              ,mall_shop_id
              ,mall_shop_name              
              ,sum(case when week_dt = '周末' then 1 else 0 end) as weekend_cnt
              ,sum(case when week_dt = '工作日' then 1 else 0 end) as weekday_cnt
              ,sum(case when week_dt = '周末' then user_cnt else 0 end) as weekend_user_cnt
              ,sum(case when week_dt = '工作日' then user_cnt else 0 end) as weekday_user_cnt
          from (
                select pid
                      ,mall_shop_id
                      ,mall_shop_name
                      ,dt                      
                      ,case when WEEKDAY(to_date(dt,'yyyymmdd')) in (5,6) then '周末' else '工作日' end as week_dt
                      ,user_cnt
                  from (
                         select pid
                               ,mall_shop_id
                               ,mall_shop_name
                               ,dt
                               ,sum(user_cnt) as user_cnt
                           from alipay_ddm_adm_ddm_ot_mall_hot_diagram_dd 
                          where dt between '${beg_date}' and '${end_date}'
                          group by pid
                                  ,mall_shop_id
                                  ,mall_shop_name
                                  ,dt                       
                       ) t
               ) a
         group by pid
                 ,mall_shop_id
                 ,mall_shop_name
       ) b
 group by pid
      ,mall_shop_id
      ,mall_shop_name
       
       
              
       
       
       
客流时点分布
-- 无法处理




来源地分布

--居住地、工作地  客流占比

select a.pid
      ,a.mall_shop_id
      ,a.mall_shop_name
      ,a.target_name
      ,case when t.total_user_cnt=0 then 0 else a.residence_user_cnt/t.total_user_cnt end as residence_user_rate
      ,case when t.total_user_cnt=0 then 0 else a.office_user_cnt/t.total_user_cnt end as office_user_rate
  from (
        select pid
              ,mall_shop_id
              ,mall_shop_name     
              ,sum(user_cnt) as total_user_cnt     --每个商场的总人数
          from alipay_ddm_adm_ddm_ot_mall_hot_diagram_dd
         where dt between '${beg_date}' and '${end_date}'
         group by pid
                ,mall_shop_id
                ,mall_shop_name
       ) t
  join (
        select pid
              ,mall_shop_id
              ,mall_shop_name              
              ,target_name
              ,sum(case when lbs_tag = 'home' then user_cnt else 0 end) as office_user_cnt
              ,sum(case when lbs_tag = 'work' then user_cnt else 0 end) as residence_user_cnt
          from alipay_ddm_adm_ddm_ot_mall_hot_diagram_dd
         where dt between '${beg_date}' and '${end_date}'
         group by pid
              ,mall_shop_id
              ,mall_shop_name              
              ,target_name
       ) a  
    on t.pid = a.pid and t.mall_shop_id = a.mall_shop_id and t.mall_shop_name = a.mall_shop_name
    
    
-- order by residence_user_rate desc 
-- limit 20


============================================================================

销售洞察
--交易总金额：期间内项目支付宝支付发生的交易总金额变化数和幅度，历史趋势变化图和选定时期的对比变化图
--交易笔数：期间内项目支付宝支付发生的交易总笔数变化数和幅度，历史趋势变化图和选定时期的对比变化图
--笔单价：期间内项目支付宝支付发生的交易总金额/期间交易总笔数，变化数和幅度，历史趋势变化图和选定时期的对比变化图
--客单价：期间内项目支付宝支付发生的交易总金额/（期间每天交易用户数累计值）变化数和幅度，历史趋势变化图和选定时期的对比变化图
--连单率：期间内项目支付宝支付发生的交易总笔数/（期间每天交易用户数累计值）变化数和幅度，历史趋势变化图和选定时期的对比变化图

--每天交易总金额,交易总笔数,笔单价,客单价,连单率 是多少
select PID
      ,mall_shop_id
      ,mall_shop_name
      ,biz_date
      ,sum(trade_amt) as trade_amt  --交易总金额
      ,sum(trade_cnt) as trade_cnt  --交易笔数
      ,sum(trade_amt)/sum(trade_cnt) as one_trade_price --笔单价
      ,sum(trade_amt)/count(distinct user_id) as one_user_price  --客单价
      ,sum(trade_cnt)/count(distinct user_id) as one_user_trade_cnt  --连单率
  from alipay_ddm_adm_ddm_ot_mall_user_acm_trade_dd
 where biz_date between '${beg_date}' and '${end_date}' 
 group by PID
         ,mall_shop_id
         ,mall_shop_name
         ,biz_date


--平均         
select PID
      ,mall_shop_id
      ,mall_shop_name
      ,sum(trade_amt) as total_trade_amt          --交易总金额
      ,sum(trade_amt)/count(dt) as avg_trade_amt  --平均金额
      ,sum(trade_cnt) as total_trade_cn           --交易笔数
      ,sum(trade_cnt)/count(dt) as avg_trade_cnt  --平均笔数
      ,sum(one_trade_price)/count(dt) as avg_trade_cnt_price        --平均笔单价
      ,sum(one_user_price)/count(dt) as avg_one_user_price          --平均客单价
      ,sum(one_user_trade_cnt)/count(dt) as avg_one_user_trade_cnt  --平均连单率
  from (         
        select PID
              ,mall_shop_id
              ,mall_shop_name
              ,biz_date
              ,sum(trade_amt) as trade_amt  --交易总金额
              ,sum(trade_cnt) as trade_cnt  --交易笔数
              ,sum(trade_amt)/sum(trade_cnt) as one_trade_price --笔单价
              ,sum(trade_amt)/count(distinct user_id) as one_user_price  --客单价
              ,sum(trade_cnt)/count(distinct user_id) as one_user_trade_cnt  --连单率
          from alipay_ddm_adm_ddm_ot_mall_user_acm_trade_dd
         where biz_date between '${beg_date}' and '${end_date}' 
         group by PID
                 ,mall_shop_id
                 ,mall_shop_name
                 ,biz_date
       ) a
 group by PID
         ,mall_shop_id
         ,mall_shop_name       

         


--期间内总金额对比   
select PID
      ,mall_shop_id
      ,mall_shop_name
      ,sum(trade_amt) as trade_amt  --交易总金额
      ,sum(trade_cnt) as trade_cnt  --交易笔数
      ,sum(trade_amt)/sum(trade_cnt) as trade_cnt_price --笔单价
      ,sum(trade_amt)/count(distinct user_id) as one_user_price  --客单价
      ,sum(trade_cnt)/count(distinct user_id) as one_user_trade_cnt  --连单率
  from alipay_ddm_adm_ddm_ot_mall_user_acm_trade_dd
 where biz_date between '${beg_date}' and '${end_date}' 
 group by PID
         ,mall_shop_id
         ,mall_shop_name

         
@@{yyyyMMdd}         
         
--消费业态总金额分布

--消费业态总金额分布：期间内项目支付宝支付发生的各业态交易总金额分布
select PID
      ,mall_shop_id
      ,mall_shop_name
      ,cate_name_2
      ,sum(trade_amt)/100 as trade_amt 
  from alipay_ddm_adm_ddm_ot_mall_user_acm_trade_dd
 where biz_date between '${beg_date}' and '${end_date}' 
 group by PID
      ,mall_shop_id
      ,mall_shop_name
      ,cate_name_2



      
      



         
         
         