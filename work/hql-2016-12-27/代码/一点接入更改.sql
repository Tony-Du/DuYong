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
      -- ,t1.play_device_flag	
     ,substr('${MONTH_START_DAY}',1,6) src_file_month
  from (select a.app_channel_id
              ,min(a.become_new_unix_time) become_new_unix_time
              ,a.imei
              ,a.imsi
              ,max(if(a.user_id = '-998', 1, 0)) user_id_isnull_flag
              ,max(a.day7_keep_device_flag) day7_keep_device_flag
              ,max(a.abnormal_device_flag) abnormal_device_flag
              ,max(a.month1_keep_device_flag) month1_keep_device_flag
              -- ,max(a.play_device_flag) play_device_flag
          from rptdata.fact_cpa_active_device_detail_daily a
         where a.src_file_day >= '${MONTH_START_DAY}'
           and a.src_file_day <= '${MONTH_END_DAY}'
           and (a.new_device_flag + a.month1_keep_device_flag > 0)
         group by a.app_channel_id, a.imei, a.imsi
        ) t1
 left join mscdata.dim_cpa_channel2cooperator d1
    on (t1.app_channel_id = d1.channel_id)
  left join rptdata.dim_chn d2
    on (t1.app_channel_id = d2.chn_id);
	
-- ################################################################################################################### --
-- 一点接入结算明细(月)
insert overwrite table app.cpa_settlement_detail_monthly partition(src_file_month)
select substr('${MONTH_START_DAY}',1,6) stat_month
      ,'咪咕视讯' migu_company_name
      ,d1.cooperator_name
      ,nvl(d2.chn_name,'') chn_name
      ,t1.app_channel_id
      ,t1.add_device_num
      ,t1.month1_keep_device_num
      ,'' add_device_price
      ,'' month1_keep_price
      ,'' amount
      ,'' tax_rate
      ,substr('${MONTH_START_DAY}',1,6) src_file_month
  from (select a.app_channel_id
              ,sum(a.new_device_flag) add_device_num
              ,count(distinct if(a.month1_keep_device_flag = 1, a.device_key, null)) month1_keep_device_num
          from rptdata.fact_cpa_active_device_detail_daily a
         where a.src_file_day >= '${MONTH_START_DAY}'
           and a.src_file_day <= '${MONTH_END_DAY}'
           and (a.new_device_flag + month1_keep_device_flag > 0)
           and a.abnormal_device_flag = 0
         group by a.app_channel_id
        ) t1
 left join mscdata.dim_cpa_channel2cooperator d1
    on (t1.app_channel_id = d1.channel_id)
  left join rptdata.dim_chn d2
    on (t1.app_channel_id = d2.chn_id);	
	
-- ===================================================================================================== --
-- 一点接入结算主表(月)
insert overwrite table app.cpa_settlement_total_monthly partition(src_file_month='${SRC_FILE_MONTH}')
select '${SRC_FILE_MONTH}' stat_month
      ,'咪咕视讯' migu_company_name
      ,t1.cooperator_name
      ,sum(t1.add_device_num) add_device_num
      ,sum(t1.month1_keep_device_num) month1_keep_device_num
      ,'' add_device_price
      ,'' month1_keep_price
      ,'' amount
      ,'' tax_rate
  from app.cpa_settlement_detail_monthly t1
 where t1.src_file_month = '${SRC_FILE_MONTH}'
 group by t1.cooperator_name;
 
/mnt/cpa/bigdata/settlement