--#########################################################################################################---
-- 一点接入结算明细(月)

drop table if exists app.cpa_active_device_detail_monthly;

-- create table

create table cpa_active_device_detail_monthly (				-- 一点接入有效激活清单（日）
	stat_month        string comment '统计月份',
	migu_company_name string comment '咪咕子公司',
	cooperator_name   string comment '合作伙伴公司名称',	-- 位于表：mscdata.dim_cpa_channel2cooperator
	chn_name          string comment '渠道名称',     		-- 位于表：mscdata.dim_chn					
	app_channel_id    string comment '渠道ID', 
	become_new_time   string comment '激活时间',
	product_name      string comment '产品名称',			-- 位于表：mscdata.dim_cpa_channel2cooperator
	imei              string comment 'imei', 
	imsi 			  string comment 'imsi',
	user_id_isnull_flag     int comment '是否无账号用户', 	-- 根据用户账号user_id是否为空来判断
	day7_keep_device_flag   int comment '是否7日留存用户',
	abnormal_device_flag    int comment '是否异常行为',
	month1_keep_device_flag int comment '是否上月留存用户',
	play_device_flag        int comment '是否当前周期使用用户',	
) 
partitioned by (src_file_month string)
row format delimited 
fields terminated by '|';


-- set parameters

set mapreduce.job.name=app.cpa_active_device_detail_monthly_${MONTH_START_DAY}_${MONTH_END_DAY};


-- load data

insert overwrite table app.cpa_active_device_detail_monthly partition ( src_file_month = substr('${MONTH_START_DAY}',1,6))
select
	substr('${MONTH_START_DAY}',1,6) as stat_month,
	"咪咕视讯" as migu_company_name ,
	d1.cooperator_name,
	d2.chn_name,
	t1.app_channel_id,
	t1.become_new_unix_time,
	d1.product_name,
	t1.imei,
	t1.imsi,
	t1.user_id_isnull_flag,
	t1.day7_keep_device_flag,
	t1.abnormal_device_flag,
	t1.month1_keep_device_flag,
	t1.play_device_flag
from ( select a.app_channel_id,
			  a.become_new_unix_time,
	          a.imsi,
	          a.imei,
	          if(a.user_id='-998',1,0) user_id_isnull_flag,
	          a.day7_keep_device_flag,
	          a.abnormal_device_flagg,
			  a.month1_keep_device_flag,
			  a.play_device_flag
		 from rptdata.fact_cpa_active_device_detail_daily a			-- 一点接入有效激活清单（日）
	    where a.src_file_day >= '${MONTH_START_DAY}'
		  and a.src_file_day <= '${MONTH_END_DAY}'
		  and (a.new_device_flag + a.month1_keep_device_flag =1)	-- 此处这样处理，是因为后续的报表只统计这两个字段：新增用户和次月留存用户
	 group by a.app_channel_id, a.imsi, a.imei
) t1
left join mscdata.dim_cpa_channel2cooperator d1
	   on (t1.app_channel_id = d1.channel_id)
left join mscdata.dim_chn d2
	   on (t1.app_channel_id = d2.chn_id);


--#########################################################################################################---
-- 一点接入结算明细(月) 改进版   
drop table if exists app.cpa_active_device_detail_monthly;

-- create table

create table cpa_active_device_detail_monthly (
	stat_month string comment '统计月份',
	migu_company_name string comment '咪咕子公司',
	cooperator_name string comment '合作伙伴公司名称',	-- 位于表：mscdata.dim_cpa_channel2cooperator
	chn_name string comment '渠道名称',     -- 位于表：mscdata.dim_chn					
	app_channel_id string comment '渠道ID', 
	become_new_time string comment '激活时间',
	product_name string comment '产品名称',	-- 位于表：mscdata.dim_cpa_channel2cooperator
	imei string comment 'imei', 
	imsi string comment 'imsi',
	user_id_isnull_flag int comment '是否无账号用户', -- 根据用户账号user_id是否为空来判断
	day7_keep_device_flag int comment '是否7日留存用户',
	abnormal_device_flag int comment '是否异常行为',
	month1_keep_device_flag int comment '是否上月留存用户',
	play_device_flag int comment '是否当前周期使用用户',	
) 
partitioned by (src_file_month string)
row format delimited fields terminated by '|';


-- set parameters

set mapreduce.job.name=app.cpa_active_device_detail_monthly_${MONTH_START_DAY}_${MONTH_END_DAY};


-- load data

insert overwrite table app.cpa_active_device_detail_monthly partition ( src_file_month = substring('${MONTH_START_DAY}',1,6))
select
	substring('${MONTH_START_DAY}',1,6) as stat_month,
	"咪咕视讯" as migu_company_name ,
	d1.cooperator_name,
	d2.chn_name,
	t1.app_channel_id,
	t1.become_new_time,
	d1.product_name,
	t1.imei,
	t1.imsi,
	t1.user_id_isnull_flag,
	t1.day7_keep_device_flag,
	t1.abnormal_device_flag,
	t1.month1_keep_device_flag,
	t1.play_device_flag
from ( select a.app_channel_id,
			  min(a.become_new_time) become_new_time,              -- ??
	          a.imsi,
	          a.imei,
	          max(if(a.user_id='-998',1,0)) user_id_isnull_flag,   -- ??
	          max(a.day7_keep_device_flag) day7_keep_device_flag,  -- ??
	          max(a.abnormal_device_flag) abnormal_device_flag,    -- ??
			  max(a.month1_keep_device_flag) month1_keep_device_flag,  -- ??
			  max(a.play_device_flag) play_device_flag             -- ??
		 from rptdata.fact_cpa_active_device_detail_daily a
	    where a.src_file_day >= '${MONTH_START_DAY}'
		  and a.src_file_day <= '${MONTH_END_DAY}'
		  and (a.new_device_flag + a.month1_keep_device_flag =1)   -- 此处这样处理，是因为后续的报表只统计这两个字段：新增用户和次月留存用户
	 group by a.app_channel_id, a.imsi, a.imei
) t1
left join mscdata.dim_cpa_channel2cooperator d1
	   on (t1.app_channel_id = d1.channel_id)
left join mscdata.dim_chn d2
	   on (t1.app_channel_id = d2.chn_id);	