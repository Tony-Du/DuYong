insert overwrite table rptdata.fact_cpa_active_device_detail_daily partition(src_file_day='${SRC_FILE_PAST7DAY}')
select t1.device_key
      ,t1.app_channel_id
      ,t1.become_new_unix_time
      ,t2.app_os_type
      ,t2.imei
      ,t2.imsi
      ,t2.user_id
      ,if(t4.device_key is null, 0, 1) day7_keep_device_flag
      ,case when substr(add_months(concat_ws('-',substr('${SRC_FILE_PAST7DAY}',1,4),substr('${SRC_FILE_PAST7DAY}',5,2),'01')
                                      , -1) ,1 ,7) = from_unixtime(bigint(t1.become_new_unix_time/1000),'yyyy-MM')
            then 1 else 0 end month1_keep_device_flag
      ,t1.new_cnt new_device_flag
      ,case when from_unixtime(bigint(t1.become_new_unix_time/1000),'HH') between '02' and '05'
            then 1 else 0 end abnormal_device_flag
      ,if(t3.device_key is null, 0, 1) play_device_flag
  from (select a.device_key, a.app_channel_id
              ,max(a.new_cnt) new_cnt
              ,min(nvl(a.become_new_unix_time, a.upload_unix_time)) become_new_unix_time          
          from rptdata.fact_kesheng_sdk_new_device_hourly a
         where a.src_file_day = '${SRC_FILE_PAST7DAY}'
           and a.grain_ind = '100'
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
         where d.src_file_day = '${SRC_FILE_DAY}'
           and d.grain_ind = '100'
           and from_unixtime(bigint(d.become_new_unix_time/1000),'yyyyMMdd') = '${SRC_FILE_PAST7DAY}'
         group by d.device_key
       ) t4
    on (t1.device_key = t4.device_key)
 where t2.rn = 1;
 
-- ========================================================================================================================== --
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
			  -- 等价于 count(distinct case when a.month1_keep_device_flag=1 then a.device_key else null end) month1_keep_device_num
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

 
--/mnt/cpa/bigdata/settlement
select * from app.cpa_settlement_detail_monthly limit 1000;
								chn_name							app_channel_id,add_device_num
201612	咪咕视讯	NULL	自有渠道_咪咕直播_客户端推广_营销活动_022	101800000000022	7	0					201612
201612	咪咕视讯	NULL	腾讯应用宝咪咕直播							200300270000011	2	0					201612
201612	咪咕视讯	NULL	拓展渠道_咪咕直播_客户端推广_移动门户_001	201800000000012	4	0					201612
201612	咪咕视讯	NULL	拓展渠道_咪咕直播_客户端推广_百度_002	201800000010002	5	0					201612
201612	咪咕视讯	NULL	拓展渠道_咪咕直播_客户端推广_百度_003	201800000010003	1	0					201612
201612	咪咕视讯	NULL	拓展渠道_咪咕直播_客户端推广_百度_004	201800000010004	4	0					201612
201612	咪咕视讯	NULL	拓展渠道_咪咕直播_客户端推广_百度_005	201800000010005	24	0					201612
201612	咪咕视讯	NULL	拓展渠道_咪咕直播_客户端推广_粉丝通_012	201800000010021	1	0					201612
201612	咪咕视讯	NULL	拓展渠道_咪咕直播_客户端推广_搜狗_001	201800000010030	1	0					201612
201612	咪咕视讯	NULL	拓展渠道_咪咕直播_客户端推广_搜狗_002	201800000010031	2	0					201612
201612	咪咕视讯	NULL	拓展渠道_咪咕直播_客户端推广_今日头条_001	201800000010040	3	0					201612
201612	咪咕视讯	NULL	拓展渠道_咪咕直播_客户端推广_今日头条_002	201800000010041	10	0					201612
201612	咪咕视讯	NULL	拓展渠道_咪咕直播_客户端推广_今日头条_003	201800000010042	3	0					201612
201612	咪咕视讯	NULL	拓展渠道_咪咕直播_客户端推广_今日头条_004	201800000010043	1	0					201612
201612	咪咕视讯	NULL	拓展渠道_咪咕直播_客户端推广_百度_008		201800000010047	1	0					201612
201612	咪咕视讯	NULL	拓展渠道_咪咕直播_客户端推广_今日头条_010	201800000010058	1	0					201612
201612	咪咕视讯	NULL	拓展渠道_咪咕直播_客户端推广_今日头条_012	201800000010095	4	0					201612
201612	咪咕视讯	NULL	拓展渠道_咪咕直播_客户端推广_今日头条_015	201800000010098	5	0					201612
201612	咪咕视讯	NULL	拓展渠道_咪咕直播_客户端推广_今日头条_016	201800000010099	1	0					201612
201612	咪咕视讯	NULL	拓展渠道_咪咕直播_客户端推广_百度_013		201800000010118	1	0					201612
201612	咪咕视讯	NULL	拓展渠道_咪咕直播_客户端推广_今日头条_018	201800000010141	28	0					201612
201612	咪咕视讯	NULL	拓展渠道_咪咕直播_客户端推广_今日头条_019	201800000010142	240	0					201612
201612	咪咕视讯	NULL	拓展渠道_咪咕直播_客户端推广_今日头条_021	201800000010147	1	0					201612
201612	咪咕视讯	NULL	拓展渠道_咪咕直播_客户端推广_今日头条_022	201800000010148	1	0					201612
201612	咪咕视讯	NULL	拓展渠道_咪咕直播_客户端推广_今日头条_024	201800000010150	8	0					201612
201612	咪咕视讯	NULL	拓展渠道_咪咕直播_测试_测试_002				201800020010001	15	0					201612
201612	咪咕视讯	测试公司1		DeveloperDebug										105	1					201612
201612	咪咕视讯	测试公司2		NotSetChannel										15	0					201612
201612	咪咕视讯	NULL		ceshi000												1	0					201612


select * from app.cpa_settlement_total_monthly limit 1000;

							add_device_num
201612	咪咕视讯	NULL		375	0					201612
201612	咪咕视讯	测试公司1	105	1					201612
201612	咪咕视讯	测试公司2	15	0					201612
