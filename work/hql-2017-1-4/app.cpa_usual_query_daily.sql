--一点接入日常查询(日)
set mapreduce.job.name=app.cpa_usual_query_daily_${SRC_FILE_DAY};
insert overwrite table app.cpa_usual_query_daily partition(src_file_day='${SRC_FILE_DAY}')

select
--统计日期
'20161220',
--咪咕子公司
'咪咕视讯',
--产品名称
nvl(b.product_name,'') product_name,
--合作伙伴公司名称
nvl(b.cooperator_name,'') cooperator_name,
--合作伙伴公司标识
b.cooperator_key,
--渠道名称
nvl(d.chn_name,'') chn_name,
--渠道id
c.app_channel_id,
--活跃用户数active_device_num
c.active_device_num, 
--首次激活新增用户
c.add_device_num,
--无账号用户数user_id_isnull_device_num(在新增用户的基础上筛选无账号用户)   
c.user_id_isnull_device_num,   
--异常行为用户数(在新增用户的基础上筛选异常行为用户数)
c.abnormal_device_num,
--上月留存用户数
c.month1_keep_device_num,
--使用用户数(在新增用户的基础上筛选使用用户数)
c.play_device_num,
--7日留存用户
c.day7_keep_device_num
from(select a.app_channel_id,
count(distinct(a.device_key)) active_device_num,
sum(a.new_device_flag) add_device_num,
sum(case when a.user_id = '-998' then a.new_device_flag else 0 end) user_id_isnull_device_num,
sum(case when a.abnormal_device_flag = 1 then a.new_device_flag else 0 end) abnormal_device_num,
count(distinct(case  when a.month1_keep_device_flag = 1  then a.device_key else null end)) month1_keep_device_num,
sum(case when a.play_device_flag= 1 then a.new_device_flag else 0 end) play_device_num,
count(distinct(case when a.day7_keep_device_flag = 1 then a.device_key else null end)) day7_keep_device_num
from  rptdata.fact_cpa_active_device_detail_daily a
where src_file_day = '${SRC_FILE_DAY}'
group by  a.app_channel_id
)c
left join mscdata.dim_cpa_channel2cooperator b
on  c.app_channel_id = b.channel_id
left join rptdata.dim_chn  d 
on c.app_channel_id = d.chn_id;

	 