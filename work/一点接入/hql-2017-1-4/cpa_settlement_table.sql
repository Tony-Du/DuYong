drop table if exists mscdata.dim_cpa_channel2cooperator;

/*==============================================================*/
/* Table: dim_cpa_channel2cooperator                            */
/*==============================================================*/
create table mscdata.dim_cpa_channel2cooperator
(
   umeng_name              string,
   product_name            string,
   channel_id              string,
   cooperator_short_name   string,
   cooperator_name         string,
   cooperator_key          string,
   dw_crt_day              string
);


-------------------使用用户playurl--------------------------------------------
with stg_deviceinfo as
  (select t0.app_os_type, t0.imei, t0.idfa, t0.imsi, t0.user_id, t0.phone_number
     from (select td.app_os_type, td.imei, td.idfa, td.imsi, td.user_id, td.phone_number
                 ,row_number() over (partition by td.imei, td.idfa
                                         order by td.upload_unix_time desc) rn
             from (select a.os_type_code app_os_type
                         ,if(nvl(trim(a.imei), '') = '', '-998', trim(a.imei)) imei
                         ,if(nvl(trim(a.idfa), '') = '', '-998', trim(a.idfa)) idfa
                         ,if(nvl(trim(a.imsi), '') = '', '-998', trim(a.imsi)) imsi
                         ,if(nvl(trim(a.user_id), '') = '', '-998', trim(a.user_id)) user_id
                         ,if(nvl(trim(a.phone_number), '') = '', '-998', trim(a.phone_number)) phone_number
                         ,a.upload_unix_time
                     from intdata.kesheng_sdk_device a
                    where a.src_file_day = '${SRC_FILE_DAY}'
                      and (a.os_type_code = 'AD' and nvl(trim(a.imei), '') <> ''
                            or a.os_type_code = 'iOS' and nvl(trim(a.idfa), '') <> '' )
                  ) td
          ) t0
    where t0.rn = 1
  )
  
--#########################################################################################################---
-- 一点接入有效激活清单(日)  
insert overwrite table rptdata.fact_kesheng_sdk_playurl_detail_daily partition(src_file_day='${SRC_FILE_DAY}')
select nvl(t2.device_key, -998) device_key
      ,t1.app_os_type
      ,t1.imei
      ,t1.user_id
      ,t1.imsi
      ,t1.idfa
      ,t1.app_channel_id
      ,t1.product_key
      ,t1.app_ver_code
      ,t1.app_pkg_name
      ,t1.phone_number
      ,t1.content_id
      ,t1.play_session_id
      ,t1.load_duration_ms
      ,t1.result
      ,t1.stream_ip
      ,t1.install_id
      ,t1.network_type
      ,t1.network_type1
      ,t1.network_type2
      ,count(1) load_cnt
  from (select a2.app_os_type
              ,a2.imei 
              ,a2.user_id
              ,a2.imsi
              ,a2.idfa
              ,nvl(trim(regexp_extract(a1.appchannel,'([^-]+$)',1)),'-998') app_channel_id
              ,nvl(d.product_key, -998) product_key
              ,a1.appversion app_ver_code
              ,a1.apppkg app_pkg_name
              ,a2.phone_number
              ,a1.contentid content_id
              ,a1.playsessionid play_session_id
              ,a1.loadtime load_duration_ms
              ,a1.result
              ,a1.streamip stream_ip
              ,a1.installationid install_id
              ,a1.networktype network_type
              ,a1.networktype1 network_type1
              ,a1.networktype2 network_type2
          from intdata.kesheng_sdk_playurl a1
         inner join stg_deviceinfo a2
            on (a1.os = a2.app_os_type
                and if(nvl(trim(a1.imei), '') = '', '-998', trim(a1.imei)) = a2.imei
                and if(nvl(trim(a1.idfa), '') = '', '-998', trim(a1.idfa)) = a2.idfa
               )
          left join mscdata.dim_kesheng_sdk_app_pkg d
            on (a1.apppkg = d.app_pkg_name and a1.os = d.app_os_type)
         where a1.extract_date_label = '${SRC_FILE_DAY}'
       ) t1
  left join 
       (select b.device_key, b.app_os_type, b.imei, b.user_id
              ,b.imsi ,b.idfa ,b.phone_number
          from intdata.kesheng_sdk_active_device_hourly b
         where b.src_file_day = '${SRC_FILE_DAY}'
         group by b.device_key, b.app_os_type, b.imei, b.user_id
                 ,b.imsi ,b.idfa ,b.phone_number
       ) t2
    on (t1.app_os_type = t2.app_os_type and t1.imei = t2.imei
        and t1.user_id = t2.user_id and t1.imsi = t2.imsi
        and t1.idfa = t2.idfa and t1.phone_number = t2.phone_number
       )
 group by nvl(t2.device_key, -998) ,t1.app_os_type ,t1.imei ,t1.user_id ,t1.imsi
         ,t1.idfa ,t1.app_channel_id ,t1.product_key ,t1.app_ver_code
         ,t1.app_pkg_name ,t1.phone_number ,t1.content_id ,t1.play_session_id
         ,t1.load_duration_ms ,t1.result ,t1.stream_ip ,t1.install_id
         ,t1.network_type ,t1.network_type1 ,t1.network_type2;
         
--#########################################################################################################---
-- 一点接入有效激活清单(月)
set mapreduce.job.name=app.cpa_active_device_detail_monthly_${MONTH_START_DAY}_${MONTH_END_DAY};

insert overwrite table app.cpa_active_device_detail_monthly(src_file_month=substr('${MONTH_START_DAY}',1,6))
select substr('${MONTH_START_DAY}',1,6) stat_month
      ,'咪咕视讯' migu_company_name
      ,d1.cooperator_name
      ,nvl(d2.chn_name,'') chn_name
      ,t1.app_channel_id
      ,t1.become_new_time
      ,d1.product_name
      ,t1.imei
      ,t1.imsi
      ,t1.user_id_isnull_flag
      ,t1.day7_keep_device_flag
      ,t1.abnormal_device_flag
      ,t1.month1_keep_device_flag
      ,t1.play_device_flag
  from (select a.app_channel_id
              ,max(a.become_new_time) become_new_time
              ,a.imei
              ,a.imsi
              ,max(if(a.user_id = '-998', 1, 0)) user_id_isnull_flag
              ,max(a.day7_keep_device_flag) day7_keep_device_flag
              ,max(a.abnormal_device_flag) abnormal_device_flag
              ,max(a.month1_keep_device_flag) month1_keep_device_flag
              ,max(a.play_device_flag) play_device_flag
          from rptdata.fact_cpa_active_device_detail_daily a
         where a.src_file_day >= '${MONTH_START_DAY}'
           and a.src_file_day <= '${MONTH_END_DAY}'
           and (a.new_device_flag + month1_keep_device_flag = 1)  -- ????
         group by a.app_channel_id, a.imei, a.imsi
        ) t1
 inner join mscdata.dim_cpa_channel2cooperator d1
    on (t1.app_channel_id = d1.channel_id)
  left join mscdata.dim_chn d2
    on (t1.app_channel_id = d2.chn_id);

--#########################################################################################################---
-- 一点接入结算明细(月)
set mapreduce.job.name=app.cpa_settlement_detail_monthly_${MONTH_START_DAY}_${MONTH_END_DAY};

insert overwrite table app.cpa_settlement_detail_monthly(src_file_month=substr('${MONTH_START_DAY}',1,6))
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
  from (select a.app_channel_id
              ,sum(a.new_device_flag) add_device_price
              ,sum(a.month1_keep_device_flag) month1_keep_device_num
          from rptdata.fact_cpa_active_device_detail_daily a
         where a.src_file_day >= '${MONTH_START_DAY}'
           and a.src_file_day <= '${MONTH_END_DAY}'
           and (a.new_device_flag + month1_keep_device_flag = 1)
           and a.abnormal_device_flag = 0
         group by a.app_channel_id
        ) t1
 inner join mscdata.dim_cpa_channel2cooperator d1
    on (t1.app_channel_id = d1.channel_id)
  left join mscdata.dim_chn d2
    on (t1.app_channel_id = d2.chn_id)
;

--#########################################################################################################---
-- 一点接入结算主表(月)
set mapreduce.job.name=app.cpa_settlement_total_monthly_{SRC_FILE_MONTH};

insert overwrite table app.cpa_settlement_total_monthly(src_file_month='${SRC_FILE_MONTH}')
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

-- ################################################################################################### ---
-- 一点接入日常查询(日)
set mapreduce.job.name=app.cpa_usual_query_daily_{SRC_FILE_DAY};

insert overwrite table app.cpa_usual_query_daily(src_file_day='${SRC_FILE_DAY}')
select '${SRC_FILE_DAY}' stat_day
      ,'咪咕视讯' migu_company_name
      ,d1.product_name
      ,d1.cooperator_name
      ,d1.cooperator_key
      ,d2.chn_name
      ,t1.app_channel_id
      ,t1.active_device_num
      ,t1.add_device_num
      ,t1.user_id_isnull_device_num
      ,t1.abnormal_device_num
      ,t1.month1_keep_device_num
      ,t1.play_device_num
      ,t1.day7_keep_device_num
  from (select a.app_channel_id
              ,count(distinct a.device_key) active_device_num
              ,sum(a.new_device_flag) add_device_num
              ,sum(if(a.user_id = '-998', 0, a.new_device_flag)) user_id_isnull_device_num
              ,sum(if(a.abnormal_device_flag = 0, 0, a.new_device_flag)) abnormal_device_num
              ,count(distinct if(a.month1_keep_device_flag = 1, a.device_key, null)) month1_keep_device_num
              ,sum(if(a.play_device_flag = 0, 0, a.new_device_flag)) play_device_num
              ,sum(if(a.day7_keep_device_flag = 0, 0, a.new_device_flag)) day7_keep_device_num
          from fact_cpa_active_device_detail_daily a
         where a.src_file_day = '${SRC_FILE_DAY}'
         group by a.app_channel_id
       ) t1
 inner join mscdata.dim_cpa_channel2cooperator d1
    on (t1.app_channel_id = d1.channel_id)
  left join mscdata.dim_chn d2
    on (t1.app_channel_id = d2.chn_id);

-- ################################################################################################### ---
-- 一点接入日常查询汇总(月)
set mapreduce.job.name=rptdata.fact_cap_usual_query_monthly_{MONTH_START_DAY}_{MONTH_END_DAY};

insert overwrite table rptdata.fact_cap_usual_query_monthly(src_file_month=substr('${MONTH_START_DAY}',1,6))
select t1.app_channel_id
      ,sum(t1.app_channel_id)
      ,sum(t1.add_device_num)
      ,sum(t1.active_device_num)
      ,sum(t1.month1_keep_device_num)
      ,sum(t1.month1_add_device_num)
      ,sum(t1.play_device_num)
      ,sum(t1.user_id_isnull_device_num)
      ,sum(t1.abnormal_device_num)
      ,sum(t1.day7_keep_device_num)
  from (select a.app_channel_id
              ,sum(a.new_device_flag) add_device_num
              ,count(distinct a.device_key) active_device_num
              ,count(distinct if(a.month1_keep_device_flag = 1, a.device_key, null)) month1_keep_device_num
              ,0 month1_add_device_num
              ,sum(if(a.play_device_flag = 0, 0, a.new_device_flag)) play_device_num
              ,sum(if(a.user_id = '-998', 0, a.new_device_flag)) user_id_isnull_device_num
              ,sum(if(a.abnormal_device_flag = 0, 0, a.new_device_flag)) abnormal_device_num
              ,sum(if(a.day7_keep_device_flag = 0, 0, a.new_device_flag)) day7_keep_device_num
          from rptdata.fact_cpa_active_device_detail_daily a
         where a.src_file_day >= '${MONTH_START_DAY}'
           and a.src_file_day <= '${MONTH_END_DAY}'
         group by a.app_channel_id
         union all
        select b.app_channel_id
              ,0 add_device_num
              ,0 active_device_num
              ,0 month1_keep_device_num
              ,b.add_device_num month1_add_device_num
              ,0 play_device_num
              ,0 user_id_isnull_device_num
              ,0 user_id_isnull_device_num
              ,0 abnormal_device_num
              ,0 day7_keep_device_num
          from rptdata.fact_cap_usual_query_monthly b
         where src_file_month = substr('${MONTH_START_DAY}',1,6)
       ) t1
 group by t1.app_channel_id;

-- ################################################################################################### ---
-- 一点接入日常查询汇总(月)
set mapreduce.job.name=app.cpa_usual_query_monthly_{SRC_FILE_MONTH};

insert overwrite table app.cpa_usual_query_monthly(src_file_month='{SRC_FILE_MONTH}')
select '${SRC_FILE_MONTH}' stat_month
      ,'咪咕视讯' migu_company_name
      ,d1.product_name
      ,d1.cooperator_name
      ,d1.cooperator_key
      ,d2.chn_name
      ,t1.app_channel_id
      ,t1.active_device_num
      ,t1.month1_keep_device_num
      ,if(t1.month1_add_device_num = 0, 0, 
          round(t1.t1.month1_keep_device_num*100/t1.month1_add_device_num,2)) month1_keep_pct
      ,t1.add_device_num
      ,t1.play_device_num
      ,t1.user_id_isnull_device_num
      ,if(t1.add_device_num = 0, 0, 
          round(t1.t1.user_id_isnull_device_num*100/t1.add_device_num,2)) user_id_isnull_pct
      ,t1.abnormal_device_num
      ,if(t1.add_device_num = 0, 0, 
          round(t1.t1.abnormal_device_num*100/t1.add_device_num,2)) abnormal_pct
      ,t1.day7_keep_device_num
      ,if(t1.day7_keep_device_num = 0, 0, 
          round(t1.t1.day7_keep_device_num*100/t1.add_device_num,2)) day7_keep_pct
  from fact_cap_usual_query_monthly t1
 inner join mscdata.dim_cpa_channel2cooperator d1
    on (t1.app_channel_id = d1.channel_id)
  left join mscdata.dim_chn d2
    on (t1.app_channel_id = d2.chn_id)
 where t1.src_file_month = '${SRC_FILE_MONTH}';
