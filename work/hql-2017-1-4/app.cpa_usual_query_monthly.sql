--一点接入日常查询(月)
        set mapreduce.job.name=app.cpa_usual_query_monthly_${SRC_FILE_MONTH};
    insert overwrite table app.cpa_usual_query_monthly partition(src_file_month='${SRC_FILE_MONTH}') 
	
select 
--统计日期
'201612',
--咪咕子公司
'咪咕视讯',
--产品名称
nvl(b.product_name,'') product_name,       
--合作伙伴公司名称
nvl(b.cooperator_name,'') cooperator_name, 
--合作伙伴公司标识
b.cooperator_key,
--渠道名称
nvl(c.chn_name,'') chn_name,     
--渠道id
a.app_channel_id,
--活跃用户数
a.active_device_num, 
--上月留存用户数
a.month1_keep_device_num,
--上月留存率=上月留存用户数/上月首次激活用户数（上月新增用户数）   
case when a.month1_add_device_num=0 then 0 else round(month1_keep_device_num*100/a.month1_add_device_num , 2) end,
--首次激活新增用户数
a.add_device_num,
--使用用户数
a.play_device_num,
--无账号用户数
a.user_id_isnull_device_num,
--无账号用户数比例=无账号用户数/首次激活用户数
case when a.add_device_num = 0 then 0 else round(user_id_isnull_device_num*100/a.add_device_num , 2) end,
--异常行为用户数
a.abnormal_device_num,
--异常行为用户数比例
case when a.add_device_num = 0 then 0 else round(abnormal_device_num*100/a.add_device_num , 2) end ,
--第7日留存用户
a.day7_keep_device_num ,
--第7日留存率
case when a.add_device_num = 0 then 0 else round(day7_keep_device_num*100/a.add_device_num , 2) end	 
from rptdata.fact_cap_usual_query_monthly a
left join  mscdata.dim_cpa_channel2cooperator b
on  a.app_channel_id = b.channel_id
left join rptdata.dim_chn c
on  a.app_channel_id = c.chn_id 
where  src_file_month = '${SRC_FILE_MONTH}';
	 

	 
	 
	 
	