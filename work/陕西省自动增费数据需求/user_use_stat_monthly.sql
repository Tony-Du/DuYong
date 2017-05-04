
create table app.user_busi_stat_sx0290_monthly (      --用户使用月汇总
phone_number   string,             --手机号码
city_name      string,             --地市
busi_prod_name string,             --业务产品名称，从rpt.dim_term_prod_v取
info_fee       decimal(38,4),      --信息费
login_cnt      int,                --登录频次:访问天数
duration_min   decimal(38,4),      --播放时长
flow_mb        decimal(38,4)       --播放流量
)
partitioned by (src_file_month string)
row format delimited fields terminated by '31'
stored as textfile;

--逻辑错误
insert overwrite table app.user_busi_stat_sx0290_monthly partition (src_file_month =  ${SRC_FILE_MONTH})
select a.usernum_id as phone_number
      ,b.region_name as city_name
      ,c.term_video_type_name as busi_prod_name                         --业务产品名称，left join有null值
      ,(sum(nvl(d.amount,0))+sum(nvl(e.amount,0)))/100 as info_fee      --信息费(元),有效小数限2位
      ,count(distinct a.src_file_day) as login_cnt                      --登录频次:访问天数
      ,round(sum(a.duration_sec)/60, 2) as duration_min
      ,round(sum(a.flow_kb)/1024, 2) as flow_mb
  from rptdata.fact_use_detail a   
 inner join rptdata.dim_region b
    on a.msisdn_region_id = b.region_id and b.prov_id = '0290' --陕西省的ID    
  left join rpt.dim_term_prod_v c 
    on a.termprod_id = c.term_prod_id     
  left join (
            select t1.user_id,
                   sum(t1.amount) as amount                 --单位(分)
              from rptdata.fact_ugc_order_detail_daily t1   --按次 
             where t1.src_file_day >= '${MONTH_START_DAY}' 
               and t1.src_file_day <= '${MONTH_END_DAY}' 
               and t1.period_unit = 'HOUR'
             group by t1.user_id
            ) d
    on a.usernum_id = d.user_id   
  left join (
            select t2.serv_number,
                   sum(t2.amount) as amount
              from rptdata.fact_user_month_fee_daily t2     --包月
             where t2.src_file_day >= '${MONTH_START_DAY}' 
               and t2.src_file_day <= '${MONTH_END_DAY}'
             group by t2.serv_number 
            ) e
    on a.usernum_id = e.serv_number    
 where a.src_file_day >= '${MONTH_START_DAY}'
   and a.src_file_day <= '${MONTH_END_DAY}' 
   --and a.user_type_id = '1'                         --手机号码              
 group by a.usernum_id, b.region_name, c.term_video_type_name
;

--数据量放大，group by时统计指标出现错误。
-------------------------------------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------------------------
当要从不同的表中统计指标时，使用union all


--改正版（1） 子查询多次进行相同的join，冗余
insert overwrite table app.user_busi_stat_sx0290_monthly partition (src_file_month = ${SRC_FILE_MONTH})
select a.phone_number
      ,a.city_name
      ,a.busi_prod_name
      ,round(sum(a.info_fee)/100, 2) as info_fee
      ,sum(a.login_cnt) as login_cnt
      ,round(sum(a.duration_sec)/60, 2) as duration_min
      ,round(sum(a.flow_kb)/1024, 3) as flow_mb
  from (
         select t.usernum_id as phone_number
               ,b.region_name as city_name
               ,nvl(c.term_video_type_name, '') as busi_prod_name        --业务产品名称，left join有null值
               ,0 as info_fee                                            --信息费(元),有效小数限2位
               ,0 as login_cnt                                           --登录频次:访问天数
               ,t.duration_sec
               ,t.flow_kb
           from rptdata.fact_use_detail t   
          inner join rptdata.dim_region b
             on t.msisdn_region_id = b.region_id and b.prov_id = '0290' --陕西省的ID    
           left join rpt.dim_term_prod_v c
             on t.termprod_id = c.term_prod_id  
          where t.src_file_day >= '${MONTH_START_DAY}'
            and t.src_file_day <= '${MONTH_END_DAY}' 
          --group by t.usernum_id, b.region_name, c.term_video_type_name
          
          union all
         
         select t1.user_id as phone_number
               ,b1.region_name as city_name
               ,nvl(c1.term_video_type_name, '') as busi_prod_name                   
               ,t1.amount as info_fee
               ,0 as login_cnt
               ,0 as duration_sec
               ,0 as flow_kb
           from rptdata.fact_ugc_order_detail_daily t1   --按次 
          inner join rptdata.dim_region b1
             on t1.city_id = b1.region_id and b1.prov_id = '0290' --陕西省的ID 
           left join rpt.dim_term_prod_v c1
             on t1.term_prod_id = c1.term_prod_id            
          where t1.src_file_day >= '${MONTH_START_DAY}'
            and t1.src_file_day <= '${MONTH_END_DAY}'
            and t1.period_unit = 'HOUR'
          --group by t1.user_id, b1.region_name, c1.term_video_type_name
         
          union all
         
         select t2.serv_number as phone_number
               ,b2.region_name as city_name
               ,nvl(c2.term_video_type_name, '') as busi_prod_name 
               ,t2.amount as info_fee 
               ,0 as login_cnt
               ,0 as duration_sec
               ,0 as flow_kb      
           from rptdata.fact_user_month_fee_daily t2     --包月
          inner join rptdata.dim_region b2
             on t2.city_id = b2.region_id and b2.prov_id = '0290' --陕西省的ID
           left join rpt.dim_term_prod_v c2
             on t2.term_prod_id = c2.term_prod_id                
          where t2.src_file_day >= '${MONTH_START_DAY}'
            and t2.src_file_day <= '${MONTH_END_DAY}'
          --group by t2.serv_number, b2.region_name, c2.term_video_type_name
         
         union all
         
         select t3.serv_number as phone_number    
               ,b3.region_name as city_name
               ,nvl(c3.term_video_type_name, '') as busi_prod_name  
               ,0 as info_fee 
               ,count(distinct t3.src_file_day) as login_cnt
               ,0 as duration_sec
               ,0 as flow_kb           
           from rptdata.fact_user_visit_hourly t3
          inner join rptdata.dim_region b3 
             on t3.phone_city_id = b3.region_id and b3.prov_id = '0290'
           left join rpt.dim_term_prod_v c3
             on t3.term_prod_id = c3.term_prod_id   
          where t3.src_file_day >= '${MONTH_START_DAY}'
            and t3.src_file_day <= '${MONTH_END_DAY}'
          group by t3.serv_number, b3.region_name, nvl(c3.term_video_type_name, '')
       ) a
group by a.phone_number, a.city_name, a.busi_prod_name;



--改正版（2），login_cnt统计错误，继续采用（1）版本	
insert overwrite table app.user_busi_stat_sx0290_monthly partition (src_file_month = ${SRC_FILE_MONTH})
select t.phone_number
      ,b.region_name as city_name
      ,c.term_video_type_name as busi_prod_name
      ,t.info_fee
      ,t.login_cnt
      ,t.duration_min
      ,t.flow_mb
  from (   
       select a.phone_number
             ,a.city_id
             ,a.term_prod_id
             ,round(sum(a.info_fee)/100, 2) as info_fee
             ,sum(a.login_cnt) as login_cnt
             ,round(sum(a.duration_sec)/60, 2) as duration_min
             ,round(sum(a.flow_kb)/1024, 3) as flow_mb
         from (
                select t1.usernum_id as phone_number
                      ,t1.msisdn_region_id as city_id
                      ,t1.termprod_id as term_prod_id
                      ,0 as info_fee                                  --信息费(元),有效小数限2位
                      ,0 as login_cnt                                 --登录频次:访问天数
                      ,t1.duration_sec
                      ,t1.flow_kb
                  from rptdata.fact_use_detail t1   
                 where t1.src_file_day >= '${MONTH_START_DAY}'
                   and t1.src_file_day <= '${MONTH_END_DAY}'
                 --group by t1.usernum_id, t1.msisdn_region_id, t1.termprod_id
                 
                 union all
                
                select t2.user_id as phone_number
                      ,t2.city_id 
                      ,t2.term_prod_id  
                      ,t2.amount as info_fee
                      ,0 as login_cnt
                      ,0 as duration_sec
                      ,0 as flow_kb
                  from rptdata.fact_ugc_order_detail_daily t2   --按次    流程中没找到，即依赖没有找到          
                 where t2.src_file_day >= '${MONTH_START_DAY}'
                   and t2.src_file_day <= '${MONTH_END_DAY}'
                   and t2.period_unit = 'HOUR'
                 --group by t2.user_id, t2.city_id, t2.term_prod_id
                
                 union all
                
                select t3.serv_number as phone_number 
                      ,t3.city_id 
                      ,t3.term_prod_id                  
                      ,t3.amount as info_fee 
                      ,0 as login_cnt
                      ,0 as duration_sec
                      ,0 as flow_kb      
                  from rptdata.fact_user_month_fee_daily t3     --包月                
                 where t3.src_file_day >= '${MONTH_START_DAY}'
                   and t3.src_file_day <= '${MONTH_END_DAY}'
                 --group by t3.serv_number, t3.city_id, t3.term_prod_id
                
                union all
                
                select t4.serv_number as phone_number
                      ,t4.phone_city_id as city_id
                      ,t4.term_prod_id                      
                      ,0 as info_fee 
                      ,count(distinct t4.src_file_day) as login_cnt
                      ,0 as duration_sec
                      ,0 as flow_kb           
                  from rptdata.fact_user_visit_hourly t4        --流程中没找到,即依赖没有找到  
                 where t4.src_file_day >= '${MONTH_START_DAY}'
                   and t4.src_file_day <= '${MONTH_END_DAY}'
                 group by t4.serv_number, t4.phone_city_id, t4.term_prod_id
              ) a
        group by a.phone_number, a.city_id, a.term_prod_id
       ) t
 inner join rptdata.dim_region b
    on t.city_id = b.region_id and b.prov_id = '0290' --陕西省的ID    
  left join rpt.dim_term_prod_v c
    on t.term_prod_id = c.term_prod_id;	
	


	
    
--数据源
rptdata.fact_ugc_order_detail_daily   按次
rptdata.fact_user_month_fee_daily   包月

desc rptdata.fact_ugc_order_detail_daily;
OK
user_type               string                     --用户类型,默认1，注册用户          
network_type            string                     --网络类型          
cp_id                   string                     --cp_id          
use_type                string                     --使用方式          
term_prod_id            string                     --终端产品ID          
term_version_id         string                     --终端产品版本id          
chn_id                  string                     --渠道ID          
city_id                 string                     --地市          
sub_busi_id             string                     --子业务          
user_id                 string                     --用户标识 (电话号码)         
period_unit             string                     --周期单位          
pay_chn_type            string                     --支付渠道类型          
order_status            string                     --订单状态          
authorize_type          string                     --授权类型          
currency                string                     --货币类型（现金或者券）          
company_id              string                     --合作公司          
goods_type              string                     --商品类型          
product_id              string                               
product_type            string                              
main                    string                     --是否主交付物          
program_id              string                               
opr_time                string                               
amount                  decimal(38,4)              --支付金额           
src_file_day            string      

select t.user_id, 
       b.region_name, 
       t.amount 
  from rptdata.fact_ugc_order_detail_daily t 
  left join rptdata.dim_region b
    on t.city_id = b.region_id
 where t.src_file_day = '20170301' 
 limit 1000;

desc rptdata.fact_user_month_fee_daily;
OK
serv_number             string                                      
user_type               string                                      
network_type            string                                      
cp_id                   string                                      
use_type                string                                      
term_prod_id            string   --!!                                  
term_version_id         string                                      
chn_id                  string                                      
city_id                 string                                      
sub_busi_id             string                                      
period_unit             string                                      
order_status            string                                      
authorize_type          string                                      
currency                string                                      
pay_chn_type            string                                      
company_id              string                                      
goods_type              string                                      
main                    string                                      
is_add                  string                                      
amount                  decimal(38,4)                               
src_file_day            string    

desc rptdata.dim_region;
OK
prov_id                 string                                      
prov_name               string                                      
region_id               string                                      
region_name             string                                      
dw_create_by            string                                      
dw_create_time          string                                      
dw_update_by            string                                      
dw_update_time          string                                      
dw_delete_flag          string                  Y/N                 
dw_crc                  string    

desc rpt.dim_term_prod_v ;
OK
term_prod_id            string  --!!                                    
term_prod_name          string                                      
term_prod_type_id       string                                      
term_prod_type_name     string                                      
term_prod_class_id      string                                      
term_prod_class_name    string                                      
term_os_type_id         string                                      
term_os_type_name       string                                      
term_video_type_id      string                                      
term_video_type_name    string                                      
term_video_soft_id      string                                      
term_video_soft_name    string         
 
--测试
insert overwrite table app.user_busi_stat_monthly partition (src_file_month = '201703' )
select a.phone_number
      ,a.city_name
      ,a.busi_prod_name
      ,round(sum(a.info_fee)/100, 2) as info_fee
      ,sum(a.login_cnt) as login_cnt
      ,round(sum(a.duration_sec)/60, 2) as duration_min
      ,round(sum(a.flow_kb)/1024, 3) as flow_mb
  from (
         select t.usernum_id as phone_number
               ,b.region_name as city_name
               ,c.term_video_type_name as busi_prod_name        --业务产品名称，left join有null值
               ,0 as info_fee                                  --信息费(元),有效小数限2位
               ,0 as login_cnt                                 --登录频次:访问天数
               ,t.duration_sec
               ,t.flow_kb
           from rptdata.fact_use_detail t   
          inner join rptdata.dim_region b
             on t.msisdn_region_id = b.region_id and b.prov_id = '0290' --陕西省的ID    
           left join rpt.dim_term_prod_v c
             on t.termprod_id = c.term_prod_id  
          where t.src_file_day >= 20170301
            and t.src_file_day <= 20170331   
          --group by t.usernum_id, b.region_name, c.term_video_type_name
          
          union all
         
         select t1.user_id as phone_number
               ,b1.region_name as city_name
               ,c1.term_video_type_name as busi_prod_name                   
               ,t1.amount as info_fee
               ,0 as login_cnt
               ,0 as duration_sec
               ,0 as flow_kb
           from rptdata.fact_ugc_order_detail_daily t1   --按次 
          inner join rptdata.dim_region b1
             on t1.city_id = b1.region_id and b1.prov_id = '0290' --陕西省的ID 
           left join rpt.dim_term_prod_v c1
             on t1.term_prod_id = c1.term_prod_id            
          where t1.src_file_day >= 20170301
            and t1.src_file_day <= 20170331
            and t1.period_unit = 'HOUR'
          --group by t1.user_id, b1.region_name, c1.term_video_type_name
         
          union all
         
         select t2.serv_number as phone_number
               ,b2.region_name as city_name
               ,c2.term_video_type_name as busi_prod_name                   
               ,t2.amount as info_fee 
               ,0 as login_cnt
               ,0 as duration_sec
               ,0 as flow_kb      
           from rptdata.fact_user_month_fee_daily t2     --包月
          inner join rptdata.dim_region b2
             on t2.city_id = b2.region_id and b2.prov_id = '0290' --陕西省的ID
           left join rpt.dim_term_prod_v c2
             on t2.term_prod_id = c2.term_prod_id                
          where t2.src_file_day >= 20170301
            and t2.src_file_day <= 20170331
          --group by t2.serv_number, b2.region_name, c2.term_video_type_name
         
         union all
         
         select t3.serv_number as phone_number    
               ,b3.region_name as city_name
               ,c3.term_video_type_name as busi_prod_name                   
               ,0 as info_fee 
               ,count(distinct t3.src_file_day) as login_cnt
               ,0 as duration_sec
               ,0 as flow_kb           
           from rptdata.fact_user_visit_hourly t3
          inner join rptdata.dim_region b3 
             on t3.phone_city_id = b3.region_id and b3.prov_id = '0290'
           left join rpt.dim_term_prod_v c3
             on t3.term_prod_id = c3.term_prod_id   
          where t3.src_file_day >= 20170301
            and t3.src_file_day <= 20170331
          group by t3.serv_number, b3.region_name, c3.term_video_type_name
       ) a
group by a.phone_number, a.city_name, a.busi_prod_name
limit 1000;





insert overwrite table app.user_busi_stat_sx0290_monthly partition (src_file_month = 201703)
select a.phone_number
      ,a.city_name
      ,a.busi_prod_name
      ,round(sum(a.info_fee)/100, 2) as info_fee
      ,sum(a.login_cnt) as login_cnt
      ,round(sum(a.duration_sec)/60, 2) as duration_min
      ,round(sum(a.flow_kb)/1024, 3) as flow_mb
  from (
         select t.usernum_id as phone_number
               ,b.region_name as city_name
               ,nvl(c.term_video_type_name, '') as busi_prod_name        --业务产品名称，left join有null值
               ,0 as info_fee                                            --信息费(元),有效小数限2位
               ,0 as login_cnt                                           --登录频次:访问天数
               ,t.duration_sec
               ,t.flow_kb
           from rptdata.fact_use_detail t   
          inner join rptdata.dim_region b
             on t.msisdn_region_id = b.region_id and b.prov_id = '0290' --陕西省的ID    
           left join rpt.dim_term_prod_v c
             on t.termprod_id = c.term_prod_id  
          where t.src_file_day >= 20170301
            and t.src_file_day <= 20170331
          --group by t.usernum_id, b.region_name, c.term_video_type_name
          
          union all
         
         select t1.user_id as phone_number
               ,b1.region_name as city_name
               ,nvl(c1.term_video_type_name, '') as busi_prod_name                   
               ,t1.amount as info_fee
               ,0 as login_cnt
               ,0 as duration_sec
               ,0 as flow_kb
           from rptdata.fact_ugc_order_detail_daily t1   --按次 
          inner join rptdata.dim_region b1
             on t1.city_id = b1.region_id and b1.prov_id = '0290' --陕西省的ID 
           left join rpt.dim_term_prod_v c1
             on t1.term_prod_id = c1.term_prod_id            
          where t1.src_file_day >=  20170301
            and t1.src_file_day <= 20170331
            and t1.period_unit = 'HOUR'
          --group by t1.user_id, b1.region_name, c1.term_video_type_name
         
          union all
         
         select t2.serv_number as phone_number
               ,b2.region_name as city_name
               ,nvl(c2.term_video_type_name, '') as busi_prod_name 
               ,t2.amount as info_fee 
               ,0 as login_cnt
               ,0 as duration_sec
               ,0 as flow_kb      
           from rptdata.fact_user_month_fee_daily t2     --包月
          inner join rptdata.dim_region b2
             on t2.city_id = b2.region_id and b2.prov_id = '0290' --陕西省的ID
           left join rpt.dim_term_prod_v c2
             on t2.term_prod_id = c2.term_prod_id                
          where t2.src_file_day >=  20170301
            and t2.src_file_day <= 20170331
          --group by t2.serv_number, b2.region_name, c2.term_video_type_name
         
         union all
         
         select t3.serv_number as phone_number    
               ,b3.region_name as city_name
               ,nvl(c3.term_video_type_name, '') as busi_prod_name  
               ,0 as info_fee 
               ,count(distinct t3.src_file_day) as login_cnt
               ,0 as duration_sec
               ,0 as flow_kb           
           from rptdata.fact_user_visit_hourly t3
          inner join rptdata.dim_region b3 
             on t3.phone_city_id = b3.region_id and b3.prov_id = '0290'
           left join rpt.dim_term_prod_v c3
             on t3.term_prod_id = c3.term_prod_id   
          where t3.src_file_day >=  20170301
            and t3.src_file_day <= 20170331
          group by t3.serv_number, b3.region_name, nvl(c3.term_video_type_name, '')
       ) a
group by a.phone_number, a.city_name, a.busi_prod_name;
