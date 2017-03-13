
CREATE TABLE `intdata.kesheng_sdk_session_start`(		
  `device_id` string, 		-- concat_ws('_', nvl(installationID, ''),  case when v.os = 'iOS' then  nvl(v.idfa, '') when v.os = 'AD' then nvl(v.imei, '') end)
  `os` string, 
  `imei` string, 
  `imsi` string, 
  `app_pkg_name` string, 
  `idfa` string, 
  `idfv` string, 
  `app_ver_code` string, 
  `os_ver_code` string, 
  `app_ver_name` string, 
  `phone_number` string, 
  `install_id` string, 
  `first_launch_channel_id` string, 		-- 有以“-”分成的三段或只有一段；如果有三段则取最后一段；
  `user_id` string, 
  `account` string, 
  `start_unix_time` bigint, 
  `client_id` string)
PARTITIONED BY ( 
  `src_file_day` string, 
  `src_file_hour` string)
CLUSTERED BY ( 
  device_id) 
INTO 16 BUCKETS

-- ############################################################################################################################################################################### --

drop view if exists int.kesheng_sdk_session_start_cpa_v;
create view int.kesheng_sdk_session_start_cpa_v as
select a1.imei
	  ,a1.user_id
	  ,a1.idfa
	  ,a1.imsi
	  ,a1.app_ver_code
	  ,a1.phone_number 
      ,a1.app_pkg_name
      ,if(a1.app_channel_id = '', '-998', a1.app_channel_id) app_channel_id
      ,a1.app_os_type
	  ,a1.start_unix_time
	  ,a1.install_id
	  ,a1.first_launch_channel_id
      ,a1.src_file_day
	  ,a1.src_file_hour
  from (select  if(nvl(trim(a.imei),'') = '', '-998', trim(a.imei)) imei, 
                if(nvl(trim(a.user_id),'') = '', '-998', trim(a.user_id)) user_id,
                if(nvl(trim(a.idfa),'') = '', '-998', trim(a.idfa)) idfa,
                if(nvl(trim(a.imsi),'') = '', '-998', trim(a.imsi)) imsi,
				
				-- 这里做了一个处理，imei、user_id、idfa、imsi如果为NULL或者为空字符串，赋值为 '-998'
				
                a.app_ver_code,
                if(nvl(trim(a.phone_number),'') = '', '-998', trim(a.phone_number)) phone_number,
                a.app_pkg_name,
                a.first_launch_channel_id,
                nvl(trim(regexp_extract(a.first_launch_channel_id,'([^-]+$)',1)),'-998') app_channel_id, -- [^-]：除了以-开头的任意一个，+：匹配前面的子表达式一次或者多次，$: 行的结尾 
                a.os as app_os_type,
                a.start_unix_time,
                a.install_id,
                a.src_file_day,
                a.src_file_hour
          from intdata.kesheng_sdk_session_start a
       ) a1;

-- ############################################################################################################################################################################### --

insert overwrite table rptdata.fact_kesheng_sdk_session_start_hourly partition(src_file_day='${EXTRACT_DATE}', src_file_hour='${EXTRACT_HOUR}')
select nvl(t2.device_key,-998)
	  ,t1.imei
	  ,t1.user_id
	  ,t1.idfa
	  ,t1.imsi
	  ,t1.app_ver_code
      ,t1.app_pkg_name
	  ,t1.phone_number
	  ,t1.app_os_type
      ,t1.install_id
	  ,t1.first_launch_channel_id
	  ,t1.app_channel_id
      ,t1.product_key
	  ,count(1) as start_cnt		-- 启动次数的算法,(例如：启动次数为3次,则有3条相同的记录)
  from (select a1.imei
			  ,a1.user_id
			  ,a1.idfa
			  ,a1.imsi
			  ,a1.app_ver_code
              ,a1.app_pkg_name
			  ,a1.phone_number
              ,a1.app_os_type
			  ,a1.install_id
			  ,a1.first_launch_channel_id
              ,a1.app_channel_id 
			  ,nvl(d1.product_key,-998) product_key  
          from int.kesheng_sdk_session_start_cpa_v a1
          left join mscdata.dim_kesheng_sdk_app_pkg d1	-- 此处的 left join 是为了得到 d1.product_key 
            on (d1.app_os_type = a1.app_os_type and d1.app_pkg_name = a1.app_pkg_name)
         where a1.src_file_day = '${EXTRACT_DATE}'
           and a1.src_file_hour = '${EXTRACT_HOUR}'
       ) t1
  left join			-- join这个表是为了取device_key
       (select * from intdata.kesheng_sdk_active_device_hourly b2
         where b2.src_file_day = '${EXTRACT_DATE}'
       ) t2
    on (t1.app_os_type = t2.app_os_type and t1.imei = t2.imei and t1.user_id = t2.user_id		-- 只要一个条件不符合，t2.device_key取null
          and t1.idfa = t2.idfa and t1.imsi = t2.imsi and t1.phone_number = t2.phone_number
        )
 group by nvl(t2.device_key,-998), t1.imei, t1.user_id, t1.idfa, t1.imsi, t1.app_ver_code
         ,t1.app_pkg_name, t1.app_channel_id, t1.phone_number, t1.app_os_type
         ,t1.install_id, t1.first_launch_channel_id, t1.product_key;
		 
	