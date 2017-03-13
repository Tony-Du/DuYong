
-- create table
create table  intdata.one_link_effective_active_device_daily(
device_key bigint,
app_channel_id string,
upload_time string,
product_key int,
imei string,
imsi string,
user_id string,
day7_keep_device_flag int,
month1_keep_device_flag int,
new_device_flag int,
abnormal_device_flag int,
play_flag int
)
partitioned by (src_file_day string)
row format delimited fileds terminated by '';

--- 步骤1
set mapreduce.job.name=intdata.one_link_effective_active_device_daily_${SRC_FILE_DAY};
set hive.groupby.skewindata=true;
set hive.optimize.skewjoin=true;



insert overwrite table intdata.one_link_effective_active_device_daily partition(src_file_day='${SRC_FILE_DAY}')
select
	t1.device_key, 
	t1.app_channel_id,
	t1.upload_unix_time,
	t1.product_key,
	t2.imei,
	t2.imsi,
	t2.user_id,
	case t3.device_key when null 
	        then 0 else 1 end as day7_keep_device_flag,         --- 是否7日留存用户
	case when t1.become_new_unix_time < unix_timestamp(concat(substring('${SRC_FILE_DAY}',1,6),'01'))  YYYYMM
	        and t1.become_new_unix_time >= case when substring('${SRC_FILE_DAY}',5,2) = '01'   MM					YYYY
			                                                         then unix_timestamp(cast(cast(concat(substring('${SRC_FILE_DAY}',1,4),'1201') as int)-10000 as string),'yyyyMMdd')
																	 else unix_timestamp(cast(cast(concat(substring('${SRC_FILE_DAY}',1,6),'01') as int)-100 as string),'yyyyMMdd')
																	 end										YYYYMM					
			then 1 else 0 end as month1_keep_device_flag,       --- 是否上月留存用户：小于该月的1号，大于等于上月的1号
	t1.new_cnt,                                                              --- 是否当前周期新增用户
	case when substring(from_unixtime(t1.become_new_unix_time,'yyyyMMddHH'),9,2) between 2 and 5
																	HH
			then 1 else 0 end as abnormal_device_flag,          --- 是否异常用户
	case  t4.device_key when null
	        then 0 else 1 end as play_flag                      --- 是否使用用户
from
	(
	 select device_key,app_channel_id,upload_unix_time,product_key
	 from     rptdata.fact_kesheng_sdk_new_device_hourly  -- 新设备 小时表
	)t1
left join
	intdata.kesheng_sdk_active_device_hourly t2     -- 活跃设备 小时表
	on(t1.device_key=t2.device_key)
left join
	(
	 select device_key
	 from rptdata.fact_kesheng_sdk_new_device_hourly  
	 where src_file_day=from_unixtime(unix_timestamp('${SRC_FILE_DAY}')-60*60*24*7,'yyyyMMdd')  -- 如果${SRC_FILE_DAY}传得是第7日的数据，那么t1 和t2表是7天前${SRC_FILE_DAY}-60*60*24*7的数据
	 group by device_key  
	) t3 
	on (t1.device_key=t3.device_key)
left join
     (
	  select device_key
	  from rptdata.kesheng_sdk_playurl_detail_daily     -- 播放URL明细表（日）
	  where src_file_day='${SRC_FILE_DAY}'
	  group by device_key
	 ) t4
    on (t1.device_key=t4.device_key)
where t1.src_file_day='${SRC_FILE_DAY}' and t2.src_file_day='${SRC_FILE_DAY}';		


次月留存用户 小于本月第一天  
时间戳是毫秒
使用用户去重（第一张表）
ROW_NUMBER over 最新的 注册时间
DDL-APP 制定分割符 E:\GitDorm\bigdata\BATCH\M400_Development\DB\APP\DDL

select 
row_number() OVER (PARTITION BY t2.device_key ORDER BY t1.become_new_unix_time) rank,
t2.device_key
from 
rptdata.fact_kesheng_sdk_new_device_hourly t1
left join
intdata.kesheng_sdk_active_device_hourly t2
on(t1.device_key=t2.device_key)
where t1.src_file_day='20161220' and t2.src_file_day='20161220' and rank=1
limit 50;