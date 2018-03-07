
====销售洞察========================================================================================

create table mallcoo_koubei_mall_user_acm_trade_d (
PID STRING ,
mall_shop_id STRING ,
mall_shop_name STRING ,
user_id STRING ,
user_type STRING ,
cate_name_2 STRING ,
biz_date STRING ,
trade_hour BIGINT ,
trade_amt BIGINT ,
trade_cnt BIGINT 
)
partitioned by (dt STRING )  --指消费日期


--不做处理，仅同步阿里综合体用户累计消费汇总表 ，按消费时间分区
insert overwrite table mallcoo_koubei_mall_user_acm_trade_d partition (dt = @@{yyyyMMdd})
select PID 
      ,mall_shop_id 
      ,mall_shop_name 
      ,user_id 
      ,user_type 
      ,cate_name_2 
      ,biz_date 
      ,trade_hour 
      ,trade_amt 
      ,trade_cnt
  from alipay_ddm_adm_ddm_ot_mall_user_acm_trade_dd
 where dt = @@{yyyyMMdd} 
   and biz_date = @@{yyyyMMdd}
 ;

dt
14
15
16
17

biz_date      
10~17 

===========

create table mallcoo_koubei_trade_commercial_type_d (
PID STRING ,
mall_shop_id STRING ,
mall_shop_name STRING ,
cate_name_2 STRING ,
trade_amt DOUBLE 
)
partitioned by (biz_date string)


--消费业态总金额分布
--消费业态总金额分布：期间内项目支付宝支付发生的各业态交易总金额分布

--每天的消费业态总金额
insert OVERWRITE table mallcoo_koubei_trade_commercial_type_d PARTITION (biz_date = @@{yyyyMMdd})
select PID
      ,mall_shop_id
      ,mall_shop_name
      ,cate_name_2
      ,sum(trade_amt)/100 as trade_amt 
  from mallcoo_koubei_mall_user_acm_trade_d
 where dt = @@{yyyyMMdd} 
 group by PID
      ,mall_shop_id
      ,mall_shop_name
      ,cate_name_2

biz_date      
10~17     
=========== 
      
create table mallcoo_koubei_trade_report_daily_d (
PID STRING ,
mall_shop_id STRING ,
mall_shop_name STRING ,
trade_amt  DOUBLE , 
trade_cnt  BIGINT , 
one_trade_price DOUBLE , 
one_user_price DOUBLE , 
one_user_trade_cnt DOUBLE 
)
partitioned by (biz_date STRING )



--交易总金额：期间内项目支付宝支付发生的交易总金额变化数和幅度，历史趋势变化图和选定时期的对比变化图
--交易笔数：期间内项目支付宝支付发生的交易总笔数变化数和幅度，历史趋势变化图和选定时期的对比变化图
--笔单价：期间内项目支付宝支付发生的交易总金额/期间交易总笔数，变化数和幅度，历史趋势变化图和选定时期的对比变化图
--客单价：期间内项目支付宝支付发生的交易总金额/（期间每天交易用户数累计值）变化数和幅度，历史趋势变化图和选定时期的对比变化图
--连单率：期间内项目支付宝支付发生的交易总笔数/（期间每天交易用户数累计值）变化数和幅度，历史趋势变化图和选定时期的对比变化图

--每天交易总金额,交易总笔数,笔单价,客单价,连单率 是多少
insert overwrite table mallcoo_koubei_trade_report_daily_d partition (biz_date = @@{yyyyMMdd})
select PID
      ,mall_shop_id
      ,mall_shop_name
      ,trade_amt_y as trade_amt
      ,trade_cnt
      ,trade_amt_y/trade_cnt as one_trade_price
      ,trade_amt_y/user_id_cnt as one_user_price
      ,trade_cnt/user_id_cnt as one_user_trade_cnt
  from mallcoo_koubei_trade_report_daily_m
 where biz_date = @@{yyyyMMdd}  


 
create table mallcoo_koubei_trade_report_daily_m (
PID STRING ,
mall_shop_id STRING ,
mall_shop_name STRING ,
trade_amt_y DOUBLE ,
trade_cnt BIGINT ,
user_id_cnt BIGINT 
)
partitioned by (biz_date STRING )  


insert overwrite table mallcoo_koubei_trade_report_daily_m partition (biz_date = @@{yyyyMMdd})  
select PID
     ,mall_shop_id
     ,mall_shop_name
     ,sum(trade_amt)/100 as trade_amt_y 
     ,sum(trade_cnt) as trade_cnt 
     ,count(DISTINCT user_id) as user_id_cnt
 from mallcoo_koubei_mall_user_acm_trade_d
where dt = @@{yyyyMMdd} 
group by PID
         ,mall_shop_id
         ,mall_shop_name
 
         
         
         
==== 商户评论 ===================================================================================================



create table mallcoo_koubei_mall_shop_comment_d (
PID STRING ,
mall_shop_id STRING ,
mall_shop_name STRING ,
shop_id STRING ,
shop_name STRING ,   
content STRING ,
comment_time STRING ,
comment_score BIGINT ,
comment_tag BIGINT  
)
partitioned by (comment_date STRING )



insert overwrite table mallcoo_koubei_mall_shop_comment_d partition (comment_date = @@{yyyyMMdd})
select PID
      ,mall_shop_id
      ,mall_shop_name
      ,shop_id
      ,shop_name
      ,content
      ,comment_time
      ,comment_score
      ,comment_tag
  from alipay_ddm_adm_ddm_ot_mall_shop_comment_dd
 where dt = @@{yyyyMMdd} and to_char(comment_time, 'yyyymmdd') = @@{yyyyMMdd}
 
dt
14
15
16
17


insert overwrite table mallcoo_koubei_mall_shop_comment_d partition (comment_date = @@{yyyyMMdd-1})
select PID
      ,mall_shop_id
      ,mall_shop_name
      ,shop_id
      ,shop_name
      ,content
      ,comment_time
      ,comment_score
      ,comment_tag
  from alipay_ddm_adm_ddm_ot_mall_shop_comment_dd
 where dt = @@{yyyyMMdd} and to_char(comment_time, 'yyyymmdd') = @@{yyyyMMdd-1}

 
 

====以下指标 直接在工作表发布 ======
--增量评价 
--每天 各店铺 获得的 总星数

--工作表名：  mallcoo_koubei_comment_score_daily_d 
select PID
      ,mall_shop_id
      ,mall_shop_name
      ,shop_id
      ,shop_name
      ,comment_date   
      ,sum(comment_score) as com_score
  from mallcoo_koubei_mall_shop_comment_d
 where comment_date > 20180101
 group by PID
         ,mall_shop_id
         ,mall_shop_name
         ,shop_id
         ,shop_name
         ,comment_date
    
填充数据分区 20180101
         
==========
--每天 各店铺 各评论星数 的数量 

--工作表名：  mallcoo_koubei_comment_score_cnt_d 

select PID
      ,mall_shop_id
      ,mall_shop_name
      ,shop_id
      ,shop_name
      ,comment_score    
      ,comment_date       
      ,count(comment_score) as com_score
  from mallcoo_koubei_mall_shop_comment_d
 where comment_date > 20180101
 group by PID
         ,mall_shop_id
         ,mall_shop_name
         ,shop_id
         ,shop_name
         ,comment_score
         ,comment_date

		
==========
--最近30条评论        
--工作表名： mallcoo_koubei_lastest_30_comment        

select PID
      ,mall_shop_id
      ,mall_shop_name
      ,shop_id
      ,shop_name
      ,comment_time
      ,content
      ,comment_score as com_score
      ,comment_date       
  from mallcoo_koubei_mall_shop_comment_d
 where comment_date > 20180101



-- shop_name list
select pid
      ,mall_shop_id
      ,mall_shop_name
      ,shop_id
	  ,shop_name
	  ,sum(comment_score) 
  from mallcoo_koubei_mall_shop_comment_d
 where comment_date > 20180101
group by pid
      ,mall_shop_id
      ,mall_shop_name
      ,shop_id
	  ,shop_name




 
==== 热力图指标 ===================================================================================
 

来源地分布

create table mallcoo_koubei_hot_residence_source (
pid string,
mall_shop_id string,
mall_shop_name string,
target_name string,
residence_user_rate double
)

--居住地  客流占比
insert overwrite table mallcoo_koubei_hot_residence_source
select a.pid
      ,a.mall_shop_id
      ,a.mall_shop_name
      ,a.target_name
      ,case when t.total_user_cnt=0 then 0 else a.residence_user_cnt/t.total_user_cnt end as residence_user_rate
  from (
        select pid
              ,mall_shop_id
              ,mall_shop_name     
              ,sum(user_cnt) as total_user_cnt     --每个商场的总人数
          from alipay_ddm_adm_ddm_ot_mall_hot_diagram_dd
         where dt = @@{yyyyMMdd}
         group by pid
                ,mall_shop_id
                ,mall_shop_name
       ) t
  join (
        select pid
              ,mall_shop_id
              ,mall_shop_name              
              ,target_name
              ,sum(user_cnt) as residence_user_cnt
          from alipay_ddm_adm_ddm_ot_mall_hot_diagram_dd
         where dt = @@{yyyyMMdd} 
           and lbs_tag = 'home'
           and target_name is not null
         group by pid
              ,mall_shop_id
              ,mall_shop_name              
              ,target_name
       ) a  
    on t.pid = a.pid and t.mall_shop_id = a.mall_shop_id and t.mall_shop_name = a.mall_shop_name

    
create table mallcoo_koubei_hot_office_source (
pid string,
mall_shop_id string,
mall_shop_name string,
target_name string,
office_user_rate double
)

--工作地  客流占比
insert overwrite table mallcoo_koubei_hot_office_source
select a.pid
      ,a.mall_shop_id
      ,a.mall_shop_name
      ,a.target_name
      ,case when t.total_user_cnt=0 then 0 else a.office_user_cnt/t.total_user_cnt end as office_user_rate
  from (
        select pid
              ,mall_shop_id
              ,mall_shop_name     
              ,sum(user_cnt) as total_user_cnt     --每个商场的总人数
          from alipay_ddm_adm_ddm_ot_mall_hot_diagram_dd
         where dt = @@{yyyyMMdd}
         group by pid
                ,mall_shop_id
                ,mall_shop_name
       ) t
  join (
        select pid
              ,mall_shop_id
              ,mall_shop_name              
              ,target_name
              ,sum(user_cnt) as office_user_cnt
          from alipay_ddm_adm_ddm_ot_mall_hot_diagram_dd
         where dt = @@{yyyyMMdd} 
           and lbs_tag = 'work'
           and target_name is not null
         group by pid
              ,mall_shop_id
              ,mall_shop_name              
              ,target_name
       ) a  
    on t.pid = a.pid and t.mall_shop_id = a.mall_shop_id and t.mall_shop_name = a.mall_shop_name         
         

-- 热力图       
create table mallcoo_koubei_mall_hot_diagram (
pid string,
mall_shop_id string,
mall_shop_name string,
lng double,
lat double,
user_cnt bigint
)        

insert overwrite table mallcoo_koubei_mall_hot_diagram
select pid
      ,mall_shop_id
      ,mall_shop_name
      ,lng
      ,lat
      ,sum(user_cnt) as user_cnt
  from alipay_ddm_adm_ddm_ot_mall_hot_diagram_dd
 where dt = @@{yyyyMMdd} 
   and lng is not null 
   and lat is not null
 group by pid
      ,mall_shop_id
      ,mall_shop_name
      ,lng
      ,lat
      
      


create table mallcoo_koubei_mall_hot_diagram_d (
pid string,
mall_shop_id string,
mall_shop_name string,
lng double,
lat double,
user_cnt bigint,
home_user_cnt bigint,
work_user_cnt bigint
)        


      
insert overwrite table mallcoo_koubei_mall_hot_diagram_d
select pid
      ,mall_shop_id
      ,mall_shop_name
      ,lng
      ,lat
      ,sum(user_cnt) as user_cnt
      ,sum(case when lbs_tag = 'home' then user_cnt else 0 end) as home_user_cnt
      ,sum(case when lbs_tag = 'work' then user_cnt else 0 end) as work_user_cnt
  from alipay_ddm_adm_ddm_ot_mall_hot_diagram_dd
 where dt = @@{yyyyMMdd} 
   and lng is not null and lat is not null
 group by pid
      ,mall_shop_id
      ,mall_shop_name
      ,lng
      ,lat
  