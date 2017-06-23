SELECT
    case when t1.imei is null or t1.imei = '' or t1.imei = 'null' then '-998' else t1.imei end as imei,
    case when t1.user_id is null or t1.user_id = '' or t1.user_id = 'null' then '-998' else t1.user_id end as user_id,
    case when t1.idfa is null or t1.idfa = '' or t1.idfa = 'null' then '-998' else t1.idfa end as idfa,
    case when t1.imsi is null or t1.imsi = '' or t1.imsi = 'null' then '-998' else t1.imsi end as imsi,    
    t1.app_ver_code,
    t2.app_os_type,
    nvl(t2.product_key, -998) AS product_key,
    t1.first_launch_channel_id AS app_channel_id,
    t1.start_unix_time,
    t1.src_file_day,
    t1.src_file_hour
FROM intdata.kesheng_sdk_session_start t1 LEFT JOIN mscdata.dim_kesheng_sdk_app_pkg t2 ON (t1.os = t2.app_os_type AND t1.app_pkg_name = t2.app_pkg_name)
WHERE t1.src_file_day = '20170113'
      and t1.src_file_hour = '11' limit 100;
	  
	  
select 
case when t1.imei is null or t1.imei = '' or t1.imei = 'null' then '-998' else t1.imei end imei,
case when t1.user_id is null or t1.user_id = '' or t1.user_id = 'null' then '-998' else t1.user_id end user_id,
case when t1.idfa is null or t1.idfa = '' or t1.idfa = 'null' then '-998' else t1.idfa end idfa,
case when t1.imsi is null or t1.imsi = '' or t1.imsi = 'null' then '-998' else t1.imsi end imsi,
t1.app_ver_code,
t2.app_os_type,
nvl(t2.product_key,-998) product_key,
t1.first_launch_channel_id  app_channel_id,
t1.start_unix_time,
t1.src_file_day,
t1.src_file_hour
from intdata.kesheng_sdk_session_start t1 
left join mscdata.dim_kesheng_sdk_app_pkg t2 
on (t1.os = t2.app_os_type and t1.app_pkg_name = t2.app_pkg_name)
where t1.src_file_day = '20170113' and t1.src_file_hour = '11'
limit 100;