
drop table if exists intdata.kesheng_h5_common_dy ;

create table intdata.kesheng_h5_common_dy (
url string
,os string
,time_stamp bigint
,ip_addr string
,cookie_id string
,title string
,domain string
,referrer string
,height string
,lang string
,width string
,product_id string
,page_id string
,channel_id string
)
partitioned by (src_file_day string, src_file_hour string)
stored as parquet;

--===========================================================================
create or replace view int.kesheng_h5_common_v_dy
as 
select t1.url
,t1.os
,t1.time_stamp
,t2.ip_addr
,t2.cookie_id
,t2.title
,t2.domain
,t2.referrer
,t2.height 
,t2.lang
,t2.width
,t1.product_id
,t1.page_id
,t1.channel_id
,t1.src_file_day
,t1.src_file_hour
from ods.kesheng_h5_json_ex_v t1
lateral view json_tuple(t1.param_json, 
'IP', 
'cookieId', 
'title', 
'domain', 
'referrer', 
'width', 
'height', 
'lang') t2 as 
ip_addr, 
cookie_id, 
title, 
domain, 
referrer, 
width, 
height, 
lang 
where t1.data_src_type = 'common' ;
--===========================================================================
set mapreduce.job.name=intdata.kesheng_h5_common_dy_${EXTRACT_DATE}_${EXTRACT_HOUR};
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

insert overwrite table intdata.kesheng_h5_common_dy partition (src_file_day, src_file_hour)
select url
,os
,time_stamp
,ip_addr
,cookie_id
,title
,domain
,referrer
,height 
,lang
,width
,product_id
,page_id
,channel_id
,src_file_day
,src_file_hour
from int.kesheng_h5_common_v_dy 
where src_file_day = '${EXTRACT_DATE}' and src_file_hour = '${EXTRACT_HOUR}' ;



-- 数据源=================================================
desc ods.kesheng_h5_json_ex_v;
OK
url                     string                                      
product_id              string                                      
page_id                 string                                      
channel_id              string                                      
os                      string                                      
time_stamp              string                                      
data_src_type           string                                      
param_json              string                                      
src_file_day            string                                      
src_file_hour           string 

{"type":"common"           --  ,`a1`.`param_json`
	,"IP":"127.0.0.1"                                                  
	,"cookieId":"asdf435234fqw2323vwe34"                               
	,"title":"migu movie"
	,"domain":"222.66.199.177"
	,"referrer":"222.66.199.177"
	,"width":"360"
	,"height":"640"
	,"lang":"zh-CN"}	