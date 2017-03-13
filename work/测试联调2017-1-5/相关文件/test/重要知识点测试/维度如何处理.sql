
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
			
--============================================================================================

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
			
--============================================================================================
			
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
			
--###############################################################################################

insert overwrite table rptdata.fact_kesheng_sdk_session_error_daily partition(src_file_day='${EXTRACT_DATE}')
select t0.app_channel_id, t0.product_key, t0.app_ver_code  
      ,sum(t0.start_cnt) start_cnt
      ,sum(t0.duration_ms) duration_ms
      ,sum(t0.error_cnt) error_cnt
      ,sum(t0.error_imei_num) error_imei_num
      ,sum(t0.past7day_duration_ms) past7day_duration_ms
      ,sum(t0.past7day_start_cnt) past7day_start_cnt
  from (select if(substr(a.grain_ind,1,1) = '0', '-1', a.app_channel_id) app_channel_id		--如果为零，赋为-1，不考虑该维度
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

