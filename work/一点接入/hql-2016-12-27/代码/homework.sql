
-- 一点接入有效激活清单（日）

insert overwrite table rptdata.fact_cpa_active_device_detail_daily partition(src_file_day='${SRC_FILE_PAST7DAY}')
select t1.device_key
      ,t1.app_channel_id
      ,t1.become_new_unix_time
      ,t2.app_os_type
      ,t2.imei
      ,t2.imsi
      ,t2.user_id
      ,if(t4.device_key is null, 0, 1) day7_keep_device_flag	--7日留存：当前周期内新增用户在7天后活跃 
      ,case when substr(add_months(concat_ws('-',substr('${SRC_FILE_PAST7DAY}',1,4),substr('${SRC_FILE_PAST7DAY}',5,2),'01')
                                      , -1) ,1 ,7) = from_unixtime(bigint(t1.become_new_unix_time/1000),'yyyy-MM')
            then 1 else 0 end month1_keep_device_flag	-- 次月留存:上月新增用户在当前周期内活跃   算法：当前活跃周期(src_file_day)减一个月 = 其成为新设备的时间（月）
      ,t1.new_cnt new_device_flag
      ,case when from_unixtime(bigint(t1.become_new_unix_time/1000),'HH') between '02' and '05'		
            then 1 else 0 end abnormal_device_flag		-- 当其成为新设备的时间即become_new_unix_time在凌晨2点到5点则视为 异常用户
      ,if(t3.device_key is null, 0, 1) play_device_flag		-- 是否为使用用户
  from (select a.device_key, a.app_channel_id
              ,max(a.new_cnt) new_cnt
              ,min(nvl(a.become_new_unix_time, a.upload_unix_time)) become_new_unix_time          
          from rptdata.fact_kesheng_sdk_new_device_hourly a
         where a.src_file_day = '${SRC_FILE_PAST7DAY}'
           and a.grain_ind = '100'		-- 只统计了粒度为 渠道ID 的数据
         group by a.device_key, a.app_channel_id
       ) t1
 inner join 
       (select b.device_key, b.imsi, b.user_id, b.app_os_type
              ,if(b.app_os_type = 'AD', b.imei, b.idfa) imei
              ,row_number()over(partition by b.device_key order by b.dw_upd_hour desc) rn
          from intdata.kesheng_sdk_active_device_hourly b
         where b.src_file_day = '${SRC_FILE_PAST7DAY}'         
       ) t2
    on (t1.device_key = t2.device_key)
  left join
       (select c.device_key
          from rptdata.fact_kesheng_sdk_playurl_detail_daily c
         where c.src_file_day = '${SRC_FILE_PAST7DAY}'
         group by c.device_key
       ) t3
    on (t1.device_key = t3.device_key)
  left join
       (select d.device_key         
          from rptdata.fact_kesheng_sdk_new_device_hourly d
         where d.src_file_day = '${SRC_FILE_DAY}'	 -- 当天活跃
           and d.grain_ind = '100'
           and from_unixtime(bigint(d.become_new_unix_time/1000),'yyyyMMdd') = '${SRC_FILE_PAST7DAY}'	-- 七天前为新增用户
         group by d.device_key
       ) t4
    on (t1.device_key = t4.device_key)
 where t2.rn = 1;

-- ===================================================================================================== --
-- 一点接入有效激活清单（月）

insert overwrite table app.cpa_active_device_detail_monthly partition(src_file_month)
select substr('${MONTH_START_DAY}',1,6) stat_month
      ,'咪咕视讯' migu_company_name
      ,d1.cooperator_name
      ,nvl(d2.chn_name,'') chn_name
      ,t1.app_channel_id
      ,from_unixtime(bigint(t1.become_new_unix_time/1000),'yyyyMMddHHmmss') become_new_time
      ,d1.product_name
      ,t1.imei
      ,t1.imsi
      ,t1.user_id_isnull_flag
      ,t1.day7_keep_device_flag
      ,t1.abnormal_device_flag
      ,t1.month1_keep_device_flag
      --,t1.play_device_flag	-- 是否使用用户：需要删除
      ,substr('${MONTH_START_DAY}',1,6) src_file_month
  from (select a.app_channel_id
              ,min(a.become_new_unix_time) become_new_unix_time
              ,a.imei
              ,a.imsi
              ,max(if(a.user_id = '-998', 1, 0)) user_id_isnull_flag
              ,max(a.day7_keep_device_flag) day7_keep_device_flag
              ,max(a.abnormal_device_flag) abnormal_device_flag
              ,max(a.month1_keep_device_flag) month1_keep_device_flag
              --,max(a.play_device_flag) play_device_flag
          from rptdata.fact_cpa_active_device_detail_daily a		-- 一点接入有效激活清单（日）
         where a.src_file_day >= '${MONTH_START_DAY}'
           and a.src_file_day <= '${MONTH_END_DAY}'
           and (a.new_device_flag + a.month1_keep_device_flag > 0)
         group by a.app_channel_id, a.imei, a.imsi
        ) t1
 left join mscdata.dim_cpa_channel2cooperator d1
    on (t1.app_channel_id = d1.channel_id)	--通过 渠道ID 对应到 d1表中的 合伙伙伴名称和产品名称
  left join rptdata.dim_chn d2
    on (t1.app_channel_id = d2.chn_id);	-- 通过 渠道ID 对应到d2表中的 渠道名称
	
-- ################################################################################################################### --
-- 一点接入结算明细(月)
insert overwrite table app.cpa_settlement_detail_monthly partition(src_file_month)
select substr('${MONTH_START_DAY}',1,6) stat_month
      ,'咪咕视讯' migu_company_name
      ,d1.cooperator_name
      ,nvl(d2.chn_name,'') chn_name
      ,t1.app_channel_id
      ,t1.add_device_num		--新增设备数
      ,t1.month1_keep_device_num	-- 次月留存设备数
      ,'' add_device_price
      ,'' month1_keep_price
      ,'' amount
      ,'' tax_rate
      ,substr('${MONTH_START_DAY}',1,6) src_file_month
  from (select a.app_channel_id
              ,sum(a.new_device_flag) add_device_num
              ,count(distinct if(a.month1_keep_device_flag = 1, a.device_key, null)) month1_keep_device_num
          from rptdata.fact_cpa_active_device_detail_daily a		-- 一点接入有效激活清单（日）
         where a.src_file_day >= '${MONTH_START_DAY}'
           and a.src_file_day <= '${MONTH_END_DAY}'
           and (a.new_device_flag + month1_keep_device_flag > 0)	-- 此处这样处理，是因为后续的报表只统计这两个字段：新增用户和次月留存用户
           and a.abnormal_device_flag = 0		-- 剔除异常用户
         group by a.app_channel_id				-- 以 渠道ID 分组，对应以渠道维度来结算
        ) t1
 left join mscdata.dim_cpa_channel2cooperator d1
    on (t1.app_channel_id = d1.channel_id)		--通过 渠道ID 对应到 d1表中的 合伙伙伴名称
  left join rptdata.dim_chn d2
    on (t1.app_channel_id = d2.chn_id);	-- 通过 渠道ID 对应到d2表中的 渠道名称 
	
-- ===================================================================================================== --
-- 一点接入结算主表(月)
insert overwrite table app.cpa_settlement_total_monthly partition(src_file_month='${SRC_FILE_MONTH}')
select '${SRC_FILE_MONTH}' stat_month	-- 统计月份
      ,'咪咕视讯' migu_company_name	-- 咪咕公司名称
      ,t1.cooperator_name	-- 合作公司名称
      ,sum(t1.add_device_num) add_device_num	--新增设备数
      ,sum(t1.month1_keep_device_num) month1_keep_device_num	-- 次月留存设备数
      ,'' add_device_price
      ,'' month1_keep_price
      ,'' amount
      ,'' tax_rate
  from app.cpa_settlement_detail_monthly t1
 where t1.src_file_month = '${SRC_FILE_MONTH}'
 group by t1.cooperator_name;		-- 以 合作伙伴名称 分组
 