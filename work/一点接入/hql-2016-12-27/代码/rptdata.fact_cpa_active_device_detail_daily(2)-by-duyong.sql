-- ######################################################################################### --
-- 一点接入有效激活清单(日)
drop table if exists fact_cpa_active_device_detail_daily;

create table rptdata.fact_cpa_active_device_detail_daily (
device_key bigint comment '用户标识'
,app_channel_id string comment '渠道ID'
,become_new_time string comment '激活时间'
,app_os_type string comment '客户端的操作系统类型'
,imsi string
,imei string 
,user_id string comment '用户账号'
,day7_keep_device_flag tinyint comment '是否7日留存用户'
,month1_keep_device_flag tinyint comment '是否上月留存用户'
,new_device_flag tinyint comment '是否当前周期新增用户'
,abnormal_device_flag tinyint comment '是否异常行为用户'
,play_device_flag tinyint comment '是否为使用用户'
)
partitioned by (src_file_day string)
row format delimited fields terminated by '|';


-- 当前周期内新增用户在7天后活跃   7日留存
-- 上月新增用户在当前周期内活跃   上月留存 
-- SRC_FILE_PAST7DAY 为当前周期


set mapreduce.job.name = rptdata.fact_cpa_active_device_detail_daily_'${SRC_FILE_DAY}'_'${SRC_FILE_PAST7DAY}}';

insert overwrite table rptdata.fact_cpa_active_device_detail_daily 
select   t1.device_key
		,t1.app_channel_id
		,t1.become_new_unix_time
		,t2.app_os_type
		,t2.imsi
		,t2.imei
		,t2.user_id
		,if (t4.device_key is null, 0, 1) day7_keep_device_flag -- 7日留存
		,case when from_unixtime(bigint(t1.become_new_unix_time/1000),'yyyy-MM') = substr(add_months(concat_ws('-',substr('${SRC_FILE_PAST7DAY}',1,4),
							substr('${SRC_FILE_PAST7DAY}',5,2),'01'),-1),1,7)    -- 上月为新增用户
			  then 1 else 0 end month1_keep_device_flag             -- 上月留存  
		,t1.new_cnt new_device_flag  							--  是否当前周期新增用户 1:新增 0：非新增
		,case when from_unixtime(bigint(t1.become_new_unix_time/1000),'HH') between 2 and 5     
			  then 1 else 0 end abnormal_device_flag   				-- 是否异常行为用户	
		,if (t3.device_key is null, 0, 1) play_device_flag		-- 是否为使用用户 
from 
	(select  a.device_key
			,a.app_channel_id
			,min(nvl(a.become_new_unix_time,a.upload_unix_time)) become_new_unix_time     -- become_new_unix_time这个字段存在为空的情况，当其为空时用最小的upload_unix_time代替
			,max(a.new_cnt) new_cnt
	   from rptdata.fact_kesheng_sdk_new_device_hourly a
	  where a.src_file_day = '${SRC_FILE_PAST7DAY}}'
	  group by a.device_key,a.app_channel_id
	) t1		-- 七天前新增用户信息(去重)
left join 
	(select  b.device_key 
			,b.app_os_type
			,b.imsi
			,if(b.app_os_type = 'AD', b.imei, b.idfa) imei
			,b.user_id
			,row_number() over (partition by b.device_key order by b.dw_upd_hour desc) rn
	   from intdata.kesheng_sdk_active_device_hourly b
	   where b.src_file_day='${SRC_FILE_PAST7DAY}'
	) t2        -- 七天前的活跃用户信息 (经排序后，rn=1为最近的时间)
on (t1.device_key= t2.device_key)
left join 
	(select c.device_key
	   from kesheng_sdk_playurl_detail_daily c 
	  where c.src_file_day='${SRC_FILE_PAST7DAY}'
	  group by c.device_key
	) t3   -- 七天前的播放URL（使用用户）信息
on (t1.device_key = t3.device_key)
left join 
	(select d.device_key
	from rptdata.fact_kesheng_sdk_new_device_hourly d 
   where d.src_file_day = '${SRC_FILE_DAY}'  -- 当天活跃
     and from_unixtime(bigint(d.become_new_unix_time/1000),'yyyyMMdd') = '${SRC_FILE_PAST7DAY}' -- 七天前为新增用户
	) t4  -- 当天活跃且七天前为新增用户
on (t1.device_key=t4.device_key)
where t2.rn = 1;
 
-- become_new_unix_time 是unix时间戳毫秒值 /1000 转化为秒值,
-- from_unixtime(bigint unixtime[,string pattern])函数的参数unixtime需要秒值







