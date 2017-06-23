
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
		
-- ==================================================================================== --

insert overwrite table stg.kesheng_sdk_active_device_hourly_04
select t2.device_key,		-- 如果为null，即新用户
       t1.imei,
       t1.user_id,
       t1.imsi,
       t1.idfa,
       t1.phone_number,
       coalesce (t2.dw_crt_day, t1.src_file_day) dw_crt_day,	-- 设备key生成日期。当为新增用户,t2.dw_crt_day为NULL, 所以设备key生成日期用的是t1.src_file_day，即exract_day
       coalesce (t2.dw_crt_hour, t1.src_file_hour) dw_crt_hour,
       t1.src_file_day,
       t1.src_file_hour
  from (select *
          from stg.kesheng_sdk_active_device_hourly_01 a
         where a.app_os_type = 'iOS'		-- IOS系统
       ) t1
  left join intdata.kesheng_sdk_idfa_user_device_key t2
    on t1.idfa = t2.idfa and t1.user_id = t2.user_id;

-- =============================================================================================== --

insert into table intdata.kesheng_sdk_active_device_hourly partition(src_file_day,src_file_hour)
select t1.device_key,
       'iOS' as app_os_type,
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
  from stg.kesheng_sdk_active_device_hourly_04 t1
 where t1.device_key is not null	--老用户
 union all
select dense_rank() over(order by t2.idfa, t2.user_id) + t3.device_key_max as device_key,	-- 新用户生成device_key的算法
       'iOS' as app_os_type,
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
  from (select * from stg.kesheng_sdk_active_device_hourly_04 a
         where device_key is null		-- 新用户
       ) t2 
 cross join (select coalesce(max(device_key),0) device_key_max 
               from intdata.kesheng_sdk_active_device_hourly b
              where b.src_file_day >= from_unixtime(unix_timestamp('${EXTRACT_DATE}','yyyyMMdd')-60*60*24*10,'yyyyMMdd')
            ) t3;

-- ############################################################################################################################################################################## --

insert into intdata.kesheng_sdk_idfa_user_device_key
select t5.device_key, t5.idfa, t5.user_id, t5.dw_crt_day, t5.dw_crt_hour
  from intdata.kesheng_sdk_active_device_hourly t5
  left join intdata.kesheng_sdk_idfa_user_device_key t6
 where t5.src_file_day = '${EXTRACT_DATE}'
   and t5.src_file_hour = '${EXTRACT_HOUR}'
   and t5.app_os_type = 'iOS'
   and t5.dw_crt_day = t5.src_file_day
   and t5.dw_crt_hour = t5.src_file_hour
   and t6.device_key is null
 group by t5.device_key, t5.idfa, t5.user_id, t5.dw_crt_day, t5.dw_crt_hour;
