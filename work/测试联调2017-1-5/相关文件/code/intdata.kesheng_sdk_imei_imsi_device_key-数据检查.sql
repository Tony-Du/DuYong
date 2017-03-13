
insert overwrite table stg.kesheng_sdk_active_device_hourly_01
select a.app_os_type
      ,a.imei
      ,a.user_id
      ,a.imsi
      ,a.idfa
      ,a.phone_number
      ,a.src_file_day
      ,a.src_file_hour
  from int.kesheng_sdk_session_start_cpa_v a
  left join mscdata.cpa_phone_number_blacklist b		-- 电话号码黑名单
    on (a.phone_number = b.phone_number)
 where a.src_file_day = '${EXTRACT_DATE}'				-- 20170111 
   and a.src_file_hour = '${EXTRACT_HOUR}'				-- 09
   and (a.app_os_type = 'AD' and length(a.imei) in (14,15) 
         and (a.user_id = '-998' and length(a.imsi) = 15 and a.imsi like '460%'
              or a.user_id <> '-998' and b.phone_number is null  -- 非法号码剔除
             )
        or a.app_os_type = 'iOS' and a.idfa <> '-998'
           and (a.user_id = '-998' 
                or a.user_id <> '-998' and b.phone_number is null 
               )
        )  
 group by a.app_os_type, a.imei, a.user_id, a.imsi, a.idfa
         ,a.phone_number, a.src_file_day ,a.src_file_hour;

		
-- ============================================================================================ --		

insert overwrite table stg.kesheng_sdk_active_device_hourly_03
select t2.device_key,		-- 如果为null， 即新用户
       t1.imei,
       t1.user_id,
       t1.imsi,
       t1.idfa,
       t1.phone_number,
       coalesce (t2.dw_crt_day, t1.src_file_day) dw_crt_day,
       coalesce (t2.dw_crt_hour, t1.src_file_hour) dw_crt_hour,
       t1.src_file_day,
       t1.src_file_hour
  from (select *
          from stg.kesheng_sdk_active_device_hourly_01 a
         where a.app_os_type = 'AD' and a.user_id = '-998'		-- 用户ID 为空,对应的情况是 imei + imsi
       ) t1
  left join intdata.kesheng_sdk_imei_imsi_device_key t2
    on (t1.imei = t2.imei and t1.imsi = t2.imsi);
-- ============================================================================================ --

insert into table intdata.kesheng_sdk_active_device_hourly partition(src_file_day,src_file_hour)
select t1.device_key,
       'AD' as app_os_type,
       t1.imei,
       t1.user_id,       
       t1.imsi,
       t1.idfa,
       t1.phone_number,
       t1.dw_crt_day,
       t1.dw_crt_hour,
       t1.src_file_day as dw_upd_day,
       t1.src_file_hour as dw_upd_hour,
       t1.src_file_day,
       t1.src_file_hour
  from stg.kesheng_sdk_active_device_hourly_03 t1
 where t1.device_key is not null
 union all
select dense_rank() over(order by t2.imei,t2.imsi) + t3.device_key_max as device_key,
       'AD' as app_os_type,
       t2.imei,
       t2.user_id,
       t2.imsi,
       t2.idfa,
       t2.phone_number,
       t2.dw_crt_day,
       t2.dw_crt_hour,
       t2.dw_crt_day as dw_upd_day,
       t2.dw_crt_hour as dw_upd_hour,
       t2.src_file_day,
       t2.src_file_hour
  from (select * from stg.kesheng_sdk_active_device_hourly_03 a
         where device_key is null
       ) t2
 cross join (select coalesce(max(device_key),0) device_key_max 
               from intdata.kesheng_sdk_active_device_hourly b
              where b.src_file_day >= from_unixtime(unix_timestamp('${EXTRACT_DATE}','yyyyMMdd')-60*60*24*10,'yyyyMMdd')
            ) t3;	

-- =========================================================================================================================== --

insert into intdata.kesheng_sdk_imei_imsi_device_key
select device_key, imei, imsi, dw_crt_day, dw_crt_hour
  from intdata.kesheng_sdk_active_device_hourly 
 where src_file_day  = '${EXTRACT_DATE}'
   and src_file_hour = '${EXTRACT_HOUR}'
   and app_os_type = 'AD'
   and user_id = '-998'			-- 用户ID 为空
   and dw_crt_day = src_file_day
   and dw_crt_hour = src_file_hour
 group by device_key, imei, imsi, dw_crt_day, dw_crt_hour;
	
