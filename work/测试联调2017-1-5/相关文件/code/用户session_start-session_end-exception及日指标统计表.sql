--用户session_start/session_end/exception及日指标统计表
   rptdata.fact_kesheng_sdk_session_start_hourly
   rptdata.fact_kesheng_sdk_session_end_daily
   rptdata.fact_kesheng_sdk_exception_daily
   rptdata.fact_kesheng_sdk_session_error_daily 
   rptdata.fact_kesheng_sdk_new_active_daily
   app.cpa_all_ind_daily


-- == app.cpa_all_ind_daily ==============================================================
set mapreduce.job.name=app.cpa_all_ind_daily_${SRC_FILE_DAY};

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

insert overwrite table app.cpa_all_ind_daily partition(src_file_day='${SRC_FILE_DAY}')
select '${SRC_FILE_DAY}' stat_day
      ,if(t1.product_key = -1,'-1',nvl(d1.product_name,'')) product_name
      ,t1.product_key product_key
      ,t1.app_ver_code
      ,if(t1.app_channel_id = '-1','-1',nvl(d2.chn_name,'')) chn_name
      ,t1.app_channel_id
      ,t1.accu_device_num
      ,t1.add_device_num
      ,t1.active_device_num
      ,t1.start_cnt
      ,t1.add_user_num
      ,t1.active_user_num
      ,t1.error_cnt
      ,t1.error_imei_num
      ,t1.past7day_device_num
      ,t1.past30day_device_num
      ,bigint(round(t1.past7day_duration_ms/1000)) past7day_duration_sec
      ,t1.past7day_start_cnt
      ,if(t1.all_ver_accu_device_num = 0, 0, 
           round(t1.accu_device_num*100/t1.all_ver_accu_device_num,2)) single2all_ver_accu_pct
      ,if(t1.all_channel_accu_device_num = 0, 0, 
           round(t1.accu_device_num*100/t1.all_channel_accu_device_num,2)) single2all_channel_accu_pct
      ,if(t1.start_cnt = 0, 0, 
           round(t1.error_cnt*100/t1.start_cnt,2)) error_rate
      ,if(t1.active_device_num = 0, 0, 
           round(t1.error_imei_num*100/t1.active_device_num,2)) error_imei2active_rate
      ,if(t1.day01_add_device_num = 0, 0, 
           round(t1.day01_keep_device_num*100/t1.day01_add_device_num,2)) day01_keep_pct
      ,if(t1.day03_add_device_num = 0, 0, 
           round(t1.day03_keep_device_num*100/t1.day03_add_device_num,2)) day03_keep_pct
      ,if(t1.day07_add_device_num = 0, 0, 
           round(t1.day03_keep_device_num*100/t1.day07_add_device_num,2)) day07_keep_pct
      ,if(t1.active_device_num = 0, 0, 
           round(t1.add_device_num*100/t1.active_device_num,2)) add_device_pct
      ,if(t1.active_user_num = 0, 0, 
           round(t1.add_user_num*100/t1.active_user_num,2)) add_user_pct
      ,if(t1.all_product_active_device_num = 0, 0, 
           round(t1.active_device_num*100/t1.all_product_active_device_num,2)) single2all_product_accu_pct
      ,if(t1.past7day_start_cnt = 0, 0, 
           bigint(round(t1.past7day_duration_ms/t1.past7day_start_cnt/1000,0))) avg_past7day_duration_sec
      ,if(t1.start_cnt = 0, 0, 
           bigint(round(t1.duration_ms/t1.start_cnt/1000,0))) avg_single_duration_sec
      ,if(t1.active_device_num = 0, 0, 
           round(t1.start_cnt/t1.active_device_num,2)) avg_device_start_cnt
      ,if(t1.active_device_num = 0, 0, 
           bigint(round(t1.duration_ms/t1.active_device_num/1000,0))) avg_device_duration_sec
  from stg.cpa_all_ind_daily_01 t1
  left join mscdata.dim_kesheng_sdk_product d1
    on t1.product_key = d1.product_key
  left join rptdata.dim_chn d2
    on t1.app_channel_id = d2.chn_id
 where (d1.product_key is not null or t1.product_key = -1)
   and (d2.chn_id is not null or t1.app_channel_id = '-1')
   and (t1.app_ver_code rlike '^[\\w\\.]+$' or t1.app_ver_code = '-1');

-------------------------------------------------------------------------------
   
set mapreduce.job.name=stg.cpa_all_ind_daily_01_${SRC_FILE_DAY};

set hive.merge.mapredfiles=true;

insert overwrite table stg.cpa_all_ind_daily_01
select t1.app_channel_id, t1.product_key, t1.app_ver_code
      ,sum(t1.accu_device_num)
      ,sum(t1.add_device_num)
      ,sum(t1.active_device_num)
      ,sum(t1.start_cnt)
      ,sum(t1.add_user_num)
      ,sum(t1.active_user_num)
      ,sum(t1.error_cnt)
      ,sum(t1.error_imei_num)
      ,sum(t1.past7day_device_num)
      ,sum(t1.past30day_device_num)
      ,sum(t1.past7day_duration_ms)
      ,sum(t1.past7day_start_cnt)
      ,sum(t1.all_ver_accu_device_num)
      ,sum(t1.all_channel_accu_device_num)
      ,sum(t1.day01_keep_device_num)
      ,sum(t1.day01_add_device_num)
      ,sum(t1.day03_keep_device_num)
      ,sum(t1.day03_add_device_num)
      ,sum(t1.day07_keep_device_num)
      ,sum(t1.day07_add_device_num)
      ,sum(t1.all_product_active_device_num)
      ,sum(t1.duration_ms)
  from (select a.app_channel_id
              ,a.product_key
              ,a.app_ver_code
              ,a.accu_device_num
              ,a.add_device_num
              ,a.active_device_num
              ,0 start_cnt
              ,a.add_user_num
              ,a.active_user_num
              ,0 error_cnt
              ,0 error_imei_num
              ,a.past7day_device_num
              ,a.past30day_device_num
              ,0 past7day_duration_ms
              ,0 past7day_start_cnt
              ,a.all_ver_accu_device_num
              ,a.all_channel_accu_device_num
              ,a.retention1day_device_num day01_keep_device_num
              ,a.add1day_device_num  day01_add_device_num
              ,a.retention3day_device_num day03_keep_device_num
              ,a.add3day_device_num day03_add_device_num
              ,a.retention7day_device_num day07_keep_device_num
              ,a.add7day_device_num  day07_add_device_num
              ,a.all_product_active_device_num
              ,0 duration_ms
          from rptdata.fact_kesheng_sdk_new_active_daily a
         where a.src_file_day='${SRC_FILE_DAY}'
         union all
        select b.app_channel_id
              ,b.product_key
              ,b.app_ver_code     
              ,0 accu_device_num
              ,0 add_device_num
              ,0 active_device_num
              ,b.start_cnt
              ,0 add_user_num
              ,0 active_user_num     
              ,b.error_cnt
              ,b.error_imei_num
              ,0 past7day_device_num
              ,0 past30day_device_num
              ,b.past7day_duration_ms
              ,b.past7day_start_cnt
              ,0 all_ver_accu_device_num
              ,0 all_channel_accu_device_num
              ,0 day01_keep_device_num
              ,0 day01_add_device_num
              ,0 day03_keep_device_num
              ,0 day03_add_device_num
              ,0 day07_keep_device_num
              ,0 day07_add_device_num
              ,0 all_product_active_device_num
              ,b.duration_ms
          from rptdata.fact_kesheng_sdk_session_error_daily b
         where b.src_file_day='${SRC_FILE_DAY}'
       ) t1
 group by t1.app_channel_id, t1.product_key, t1.app_ver_code;   
   
   
-- == rptdata.fact_kesheng_sdk_new_active_daily ==============================================================   

set mapreduce.job.name=rptdata.fact_kesheng_sdk_new_active_daily_${EXTRACT_DATE};

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

insert overwrite table rptdata.fact_kesheng_sdk_new_active_daily partition(src_file_day='${EXTRACT_DATE}')
select t0.app_channel_id, t0.product_key, t0.app_ver_code
      ,t0.accu_device_num	--累计设备数	
      ,t0.add_device_num	--新增设备数	
      ,t0.active_device_num	--活跃设备数	
      ,t0.add_user_num
      ,t0.active_user_num
      ,t0.past7day_device_num
      ,t0.past30day_device_num
      ,t0.retention1day_device_num
      ,t0.add1day_device_num
      ,t0.retention3day_device_num
      ,t0.add3day_device_num
      ,t0.retention7day_device_num
      ,t0.add7day_device_num
      ,t1.all_ver_accu_device_num
      ,t2.all_channel_accu_device_num
      ,t3.all_product_active_device_num
  from stg.fact_kesheng_sdk_new_active_daily_02 t0,
       (select b1.app_channel_id, b1.product_key
              ,b1.accu_device_num all_ver_accu_device_num
          from stg.fact_kesheng_sdk_new_active_daily_02 b1
         where b1.app_ver_code = '-1'
       ) t1,
       (select b2.product_key, b2.app_ver_code
              ,b2.accu_device_num all_channel_accu_device_num
          from stg.fact_kesheng_sdk_new_active_daily_02 b2
         where b2.app_channel_id = '-1'
       ) t2,
       (select b3.app_channel_id, b3.app_ver_code
              ,b3.active_device_num all_product_active_device_num
          from stg.fact_kesheng_sdk_new_active_daily_02 b3
         where b3.product_key = -1 and b3.app_ver_code = '-1'
       ) t3
 where t0.app_channel_id = t1.app_channel_id and t0.product_key = t1.product_key
   and t0.product_key = t2.product_key and t0.app_ver_code = t2.app_ver_code
   and t0.app_channel_id = t3.app_channel_id;
   
--------------------------------------------------------------------------
set mapreduce.job.name=stg.fact_kesheng_sdk_new_active_daily_02_${EXTRACT_DATE};
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

insert overwrite table stg.fact_kesheng_sdk_new_active_daily_02
select t0.app_channel_id, t0.product_key, t0.app_ver_code
      ,sum(t0.accu_device_num + t0.add_device_num) accu_device_num
      ,sum(t0.add_device_num) add_device_num
      ,sum(t0.active_device_num) active_device_num
      ,sum(t0.add_user_num) add_user_num
      ,sum(t0.active_user_num) active_user_num
      ,sum(t0.past7day_device_num) past7day_device_num
      ,sum(t0.past30day_device_num) past30day_device_num
      ,sum(t0.retention1day_device_num) retention1day_device_num
      ,sum(t0.add1day_device_num) add1day_device_num
      ,sum(t0.retention3day_device_num) retention3day_device_num
      ,sum(t0.add3day_device_num) add3day_device_num
      ,sum(t0.retention7day_device_num) retention7day_device_num
      ,sum(t0.add7day_device_num) add7day_device_num
from stg.fact_kesheng_sdk_new_active_daily_01 t0
group by t0.app_channel_id, t0.product_key, t0.app_ver_code;

--------------------------------------------------------------------------

set mapreduce.job.name=stg.fact_kesheng_sdk_new_active_daily_01_${EXTRACT_DATE}_his;

-- 取历史数据指标统计
insert into table stg.fact_kesheng_sdk_new_active_daily_01
select t3.app_channel_id, t3.product_key, t3.app_ver_code
      ,case when t3.src_file_day = from_unixtime(unix_timestamp('${EXTRACT_DATE}','yyyyMMdd')-60*60*24*1,'yyyyMMdd')
                    then t3.accu_device_num else 0 end accu_device_num
      ,0 add_device_num
      ,0 active_device_num
      ,0 add_user_num
      ,0 active_user_num
      ,0 past7day_device_num
      ,0 past30day_device_num
      ,0 retention1day_device_num
      ,case when t3.src_file_day = from_unixtime(unix_timestamp('${EXTRACT_DATE}','yyyyMMdd')-60*60*24*1,'yyyyMMdd')
                   then t3.active_device_num else 0 end add1day_device_num
      ,0 retention3day_device_num
      ,case when t3.src_file_day = from_unixtime(unix_timestamp('${EXTRACT_DATE}','yyyyMMdd')-60*60*24*3,'yyyyMMdd')
                   then t3.active_device_num else 0 end add3day_device_num
      ,0 retention7day_device_num
      ,case when t3.src_file_day = from_unixtime(unix_timestamp('${EXTRACT_DATE}','yyyyMMdd')-60*60*24*7,'yyyyMMdd')
                   then t3.active_device_num else 0 end add7day_device_num
 from rptdata.fact_kesheng_sdk_new_active_daily t3
where t3.src_file_day in(from_unixtime(unix_timestamp('${EXTRACT_DATE}','yyyyMMdd')-60*60*24*1,'yyyyMMdd')
                         ,from_unixtime(unix_timestamp('${EXTRACT_DATE}','yyyyMMdd')-60*60*24*3,'yyyyMMdd')
                         ,from_unixtime(unix_timestamp('${EXTRACT_DATE}','yyyyMMdd')-60*60*24*7,'yyyyMMdd'));

-------------------------------------------------------------------------------------------------------------------
set mapreduce.job.name=stg.fact_kesheng_sdk_new_active_daily_01_${EXTRACT_DATE}_user;

insert into table stg.fact_kesheng_sdk_new_active_daily_01
select if(substr(t2.grain_ind,1,1) = '0', '-1', t2.app_channel_id) app_channel_id
      ,if(substr(t2.grain_ind,2,1) = '0', -1, t2.product_key) product_key
      ,if(substr(t2.grain_ind,3,1) = '0', '-1', t2.app_ver_code) app_ver_code    
      ,0 accu_device_num
      ,0 add_device_num
      ,0 active_device_num
      ,sum(t2.new_cnt) add_user_num
      ,count(distinct t2.user_id) active_user_num
      ,0 past7day_device_num
      ,0 past30day_device_num
      ,0 retention1day_device_num
      ,0 add1day_device_num
      ,0 retention3day_device_num
      ,0 add3day_device_num
      ,0 retention7day_device_num
      ,0 add7day_device_num
  from rptdata.fact_kesheng_sdk_new_user_hourly t2
 where t2.src_file_day = '${EXTRACT_DATE}'
   and t2.grain_ind <> '101'
 group by if(substr(t2.grain_ind,1,1) = '0', '-1', t2.app_channel_id)
         ,if(substr(t2.grain_ind,2,1) = '0', -1, t2.product_key)
         ,if(substr(t2.grain_ind,3,1) = '0', '-1', t2.app_ver_code) ;
		 
--------------------------------------------------------------------------------------------						 
set mapreduce.job.name=stg.fact_kesheng_sdk_new_active_daily_01_${EXTRACT_DATE}_device;

insert overwrite table stg.fact_kesheng_sdk_new_active_daily_01
select if(substr(t1.grain_ind,1,1) = '0', '-1', t1.app_channel_id) app_channel_id
      ,if(substr(t1.grain_ind,2,1) = '0', -1, t1.product_key) product_key
      ,if(substr(t1.grain_ind,3,1) = '0', '-1', t1.app_ver_code) app_ver_code     
      ,0 accu_device_num
      ,sum(case when t1.src_file_day = '${EXTRACT_DATE}' 
                 then t1.new_cnt else 0 end) add_device_num
      ,count(distinct case when t1.src_file_day = '${EXTRACT_DATE}'
                 then t1.device_key else null end) active_device_num
      ,0 add_user_num
      ,0 active_user_num
      ,count(distinct case when t1.src_file_day < '${EXTRACT_DATE}'
                            and t1.src_file_day >= from_unixtime(unix_timestamp('${EXTRACT_DATE}','yyyyMMdd')-60*60*24*7,'yyyyMMdd')
                           then t1.device_key else null end) past7day_device_num
      ,count(distinct case when t1.src_file_day < '${EXTRACT_DATE}'
                           then t1.device_key else null end) past30day_device_num
      ,count(distinct case when t1.become_new_dw_day = from_unixtime(unix_timestamp('${EXTRACT_DATE}','yyyyMMdd')-60*60*24*1,'yyyy-MM-dd')
                            and t1.src_file_day = '${EXTRACT_DATE}'
                           then t1.device_key else null end) retention1day_device_num
      ,0 add1day_device_num
      ,count(distinct case when t1.become_new_dw_day = from_unixtime(unix_timestamp('${EXTRACT_DATE}','yyyyMMdd')-60*60*24*3,'yyyy-MM-dd')
                            and t1.src_file_day = '${EXTRACT_DATE}'
                           then t1.device_key else null end) retention3day_device_num
      ,0 add3day_device_num
      ,count(distinct case when t1.become_new_dw_day = from_unixtime(unix_timestamp('${EXTRACT_DATE}','yyyyMMdd')-60*60*24*7,'yyyy-MM-dd')
                            and t1.src_file_day = '${EXTRACT_DATE}'
                           then t1.device_key else null end) retention7day_device_num
      ,0 add7day_device_num
  from rptdata.fact_kesheng_sdk_new_device_hourly t1
 where t1.src_file_day  <= '${EXTRACT_DATE}'
   and t1.src_file_day  >= from_unixtime(unix_timestamp('${EXTRACT_DATE}','yyyyMMdd')-60*60*24*30,'yyyyMMdd')
   and t1.grain_ind <> '101'
 group by if(substr(t1.grain_ind,1,1) = '0', '-1', t1.app_channel_id)
         ,if(substr(t1.grain_ind,2,1) = '0', -1, t1.product_key)
         ,if(substr(t1.grain_ind,3,1) = '0', '-1', t1.app_ver_code) ;

						 
-- == rptdata.fact_kesheng_sdk_session_error_daily ===========================================================

set mapreduce.job.name=rptdata.fact_kesheng_sdk_session_error_daily_${EXTRACT_DATE};
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

insert overwrite table rptdata.fact_kesheng_sdk_session_error_daily partition(src_file_day='${EXTRACT_DATE}')
select t0.app_channel_id, t0.product_key, t0.app_ver_code  
      ,sum(t0.start_cnt) start_cnt
      ,sum(t0.duration_ms) duration_ms
      ,sum(t0.error_cnt) error_cnt
      ,sum(t0.error_imei_num) error_imei_num
      ,sum(t0.past7day_duration_ms) past7day_duration_ms
      ,sum(t0.past7day_start_cnt) past7day_start_cnt
  from (select if(substr(a.grain_ind,1,1) = '0', '-1', a.app_channel_id) app_channel_id
              ,if(substr(a.grain_ind,2,1) = '0', -1, a.product_key) product_key
              ,if(substr(a.grain_ind,3,1) = '0', '-1', a.app_ver_code) app_ver_code  
              ,a.start_cnt ,a.duration_ms ,a.error_cnt, a.error_imei_num
              ,0 past7day_start_cnt 
              ,0 past7day_duration_ms
          from stg.fact_kesheng_sdk_session_error_daily_01 a
         union all
        select b.app_channel_id, b.product_key, b.app_ver_code
              ,0 start_cnt ,0 duration_ms ,0 error_cnt ,0 error_imei_num
              ,b.start_cnt past7day_start_cnt
              ,b.duration_ms past7day_duration_ms
          from rptdata.fact_kesheng_sdk_session_error_daily b
         where b.src_file_day < '${EXTRACT_DATE}'
           and b.src_file_day >= from_unixtime(unix_timestamp('${EXTRACT_DATE}','yyyyMMdd')-60*60*24*7,'yyyyMMdd')
           and b.start_cnt + b.duration_ms > 0
       )  t0
group by t0.app_channel_id, t0.product_key, t0.app_ver_code;

---------------------------------------------------------------------------------------
set mapreduce.job.name=stg.fact_kesheng_sdk_session_error_daily_01_${EXTRACT_DATE}_exception;
insert into table stg.fact_kesheng_sdk_session_error_daily_01
select t3.app_channel_id, t3.product_key, t3.app_ver_code
      ,0 start_cnt
      ,0 duration_ms
      ,sum(t3.exception_cnt) error_cnt
      ,count(distinct case when t3.app_os_type ='AD' then t3.imei when t3.app_os_type ='iOS' then t3.idfa else null end) error_imei_num
      ,rpad(reverse(bin(cast( grouping__id as int))),3,'0') grain_ind
      ,'${EXTRACT_DATE}' src_file_day
 from rptdata.fact_kesheng_sdk_exception_daily t3
where t3.src_file_day  = '${EXTRACT_DATE}' 
group by app_channel_id, product_key, app_ver_code
grouping sets((), product_key, app_channel_id
            ,(product_key, app_ver_code), (product_key, app_channel_id)
            ,(product_key, app_ver_code, app_channel_id));

-------------------------------------------------------------------------------
set mapreduce.job.name=stg.fact_kesheng_sdk_session_error_daily_01_${EXTRACT_DATE}_end;
insert into table stg.fact_kesheng_sdk_session_error_daily_01
select t2.app_channel_id, t2.product_key, t2.app_ver_code
      ,0 start_cnt
      ,sum(t2.duration_ms) duration_ms
      ,0 error_cnt
      ,0 error_imei_num
      ,rpad(reverse(bin(cast( grouping__id as int))),3,'0') grain_ind
      ,'${EXTRACT_DATE}' src_file_day
 from rptdata.fact_kesheng_sdk_session_end_daily  t2
where t2.src_file_day  = '${EXTRACT_DATE}' 
group by app_channel_id, product_key, app_ver_code
grouping sets((), product_key, app_channel_id
            ,(product_key, app_ver_code), (product_key, app_channel_id)
            ,(product_key, app_ver_code, app_channel_id));

-------------------------------------------------------------------------------
set mapreduce.job.name=stg.fact_kesheng_sdk_session_error_daily_01_${EXTRACT_DATE}_start;
insert overwrite table stg.fact_kesheng_sdk_session_error_daily_01
select t1.app_channel_id, t1.product_key, t1.app_ver_code
      ,sum(t1.start_cnt) start_cnt
      ,0 duration_ms
      ,0 error_cnt
      ,0 error_imei_num
      ,rpad(reverse(bin(cast( grouping__id as int))),3,'0') grain_ind
      ,'${EXTRACT_DATE}' src_file_day
 from rptdata.fact_kesheng_sdk_session_start_hourly t1
where t1.src_file_day  = '${EXTRACT_DATE}'
group by app_channel_id, product_key, app_ver_code
grouping sets((), product_key, app_channel_id
            ,(product_key, app_ver_code), (product_key, app_channel_id)
            ,(product_key, app_ver_code, app_channel_id));





























 