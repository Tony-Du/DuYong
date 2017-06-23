
drop table if exists rptdata.fact_kesheng_h5_common_hourly_dy;

create table rptdata.fact_kesheng_h5_common_hourly_dy (
product_key tinyint,
channel_id string,
page_id string,
os string,
uv_num bigint,
pv_cnt bigint, 
grain_ind string
)
partitioned by (src_file_day string, src_file_hour string)
stored as parquet ;

--==========================================================================
set mapreduce.job.name=rptdata.fact_kesheng_h5_common_hourly_dy_${EXTRACT_DATE}__${EXTRACT_HOUR};
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table rptdata.fact_kesheng_h5_common_hourly_dy partition (src_file_day, src_file_hour)  
select b.product_key 
,a.channel_id 
,a.page_id 
,a.os 
,count(distinct a.ip_addr,a.cookie_id ) as uv_num
,count(1) as pv_cnt
,rpad(reverse(bin(cast( grouping__id as int))),4,'0') grain_ind
,a.src_file_day
,a.src_file_hour
from intdata.kesheng_h5_common_dy a 
left join mscdata.dim_kesheng_h5_product b on a.product_id = b.product_id 
where a.src_file_day = 20170208 and a.src_file_hour = 15 
group by product_key, channel_id, os, page_id, src_file_day, src_file_hour 
grouping sets((), product_key, channel_id, (product_key, channel_id), (product_key, os), (product_key, channel_id, os)); 
-- 对应的grouping__id 为 0(0000反转),1(1000反转),2(0100反转)
--						3(1100反转),5(1010反转),
--						7(1110反转)
'${EXTRACT_DATE}' '${EXTRACT_HOUR}' 



-- 数据源===================================
create table intdata.kesheng_h5_common_dy 
(
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

create table mscdata.dim_kesheng_h5_product d 
(
product_id           string,
product_name         string,
product_key          tinyint,
dw_crt_day           string comment '格式:YYYYMMDD'	
);