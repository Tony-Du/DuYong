drop table if exists rptdata.fact_cpa_active_device_detail_daily2;

create table rptdata.fact_cpa_active_device_detail_daily2 
(
	device_key string,
	app_channel_id   string,
	become_new_unix_time  string comment 'yyyyMMddHHMISSMS',
	imei             string,
	imsi             string,
	user_id          string,
	day7_keep_device_flag   tinyint comment '1:yes;0:no',
	month1_keep_device_flag tinyint comment '1:yes;0:no',
	new_device_flag         tinyint comment '1:yes;0:no',
	abnormal_device_flag    tinyint comment '1:yes;0:no',
	play_dabnormal_device_flagevice_flag tinyint comment '1:yes;0:no',	
) 
partitioned by (src_file_day string)
row format delimited fields terminated by '|';


insert overwrite table rptdata.fact_cpa_active_device_detail_daily2 partition (src_file_day = '${SRC_FILE_DAY}')
select 
	t1.device_key,
	t1.app_channel_id,
	t1.become_new_unix_time,
	t2.imei,
	t2.imsi,
	t2.user_id,
	case when from_unixtime(bigint(t1.become_new_unix_time/1000),'yyyyMMdd') = from_unixtime((unix_timestamp('${SRC_FILE_DAY}}','yyyyMMdd') - 7*24*60*60),'yyyyMMdd')
			then 1 else 0 end  day7_keep_device_flag,
	case when from_unixtime(bigint(t1.become_new_unix_time/1000),'yyyy-MM') = substr(add_months(concat_ws('-',substr('${SRC_FILE_DAY}}',1,4),
							substr('${SRC_FILE_DAY}}',5,2),'01') -1),1,7) 	
			then 1 else 0 end  month1_keep_device_flag,
	t1.new_cnt new_device_flag,
	case when from_unixtime(bigint(t1.become_new_unix_time/1000),'HH') between 2 and 5
			then 1 else 0 end  abnormal_device_flag,
	if (t3.device_key is null, 0, 1) play_device_flag
from
	(select 
		a.device_key,
		a.app_channel_id,
		min(nvl(a.become_new_unix_time,a.upload_unix_time)) become_new_unix_time
		a.new_cnt,
	from rptdata.fact_kesheng_sdk_new_device_hourly a    -- 新增设备信息表
	where a.src_file_day = '${SRC_FILE_DAY}' 
	group by a.device_key,a.app_channel_id  -- 去重
	) t1
left join	
	(select 
		b.imei,
		b.imsi,
		b.user_id,
	from intdata.kesheng_sdk_active_device_hourly b 
	where b.src_file_day = '${SRC_FILE_DAY}'
	) t2
on (t1.device_key = t2.device_key)
left join
	(select c.device_key
	from kesheng_sdk_playurl_detail_daily c	
	where c.src_file_day = '${SRC_FILE_DAY}'
	group by c.device_key
	) t3
on (t1.device_key = t3.device_key);	
	



-- string from_unixtime(bigint unixtime[, string foramt]) 返回值是string类型，bigint类型的unixtime参数是时间戳秒数


-- DATE add_months(DATE date, number)  DATE类型即这种形式：'2016-12-30' ,number 为整数，返回值为DATE类型

-- BIGINT unix_timestamp(string date, string format)	 返回的是秒数值,返回值是bigint类型