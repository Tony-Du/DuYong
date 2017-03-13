--一点接入日常查询汇总(月)
    set mapreduce.job.name=rptdata.fact_cap_usual_query_monthly_{MONTH_START_DAY}_{MONTH_END_DAY};
	set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.dynamic.partition=true;
 insert overwrite table rptdata.fact_cpa_usual_query_monthly partition(src_file_month)

select 
--渠道id
c.app_channel_id,
--首次激活新增用户数
sum(c.add_device_num), 
--活跃用户数
sum(c.active_device_num),  
--上月留存用户数
sum(c.month1_keep_device_num),
--上月新增用户数
sum(c.month1_add_device_num),
--使用用户数
sum(c.play_device_num),	
--无账号用户数
sum(user_id_isnull_device_num),
--异常行为用户数
sum(abnormal_device_num),
--7天留存用户数
sum(day7_keep_device_num),
substring('${MONTH_START_DAY}',1,6) src_file_month		 
from (select a.app_channel_id,
sum(new_device_flag) add_device_num,
count(distinct(a.device_key)) active_device_num,
count(distinct(case when a.month1_keep_device_flag = 1 
then a.device_key else null end))  month1_keep_device_num,
0 month1_add_device_num,
sum(case when a.play_device_flag= 1 then a.new_device_flag else 0 end) play_device_num,
sum(case when a.user_id = '-998' then a.new_device_flag else 0 end) user_id_isnull_device_num, 
sum(case when a.abnormal_device_flag = 1 then a.new_device_flag else 0 end) abnormal_device_num,
count(distinct(case when a.day7_keep_device_flag = 1 then a.device_key else null end)) day7_keep_device_num
from rptdata.fact_cpa_active_device_detail_daily a
where a.src_file_day >= '${MONTH_START_DAY}' and a.src_file_day <= '${MONTH_END_DAY}' 
group by app_channel_id
union all 
select b.app_channel_id,
0  add_device_num,
0  active_device_num,
0  month1_keep_device_num,
b.add_device_num month1_add_device_num,
0 play_device_num,
0 user_id_isnull_device_num,
0 abnormal_device_num,
0 day7_keep_device_num
from  rptdata.fact_cpa_usual_query_monthly b
where  add_device_num >0 and src_file_month= case when substring('${MONTH_START_DAY}',5,2) = '01'
then cast(cast(concat(substring('${MONTH_START_DAY}',1,4),'12') as int)-100 as String)
else cast(cast(substring('${MONTH_START_DAY}',1,6) as int)-100 as String) end 
)c
group by c.app_channel_id;	 