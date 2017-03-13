-- data question 

select src_file_day,count(1),sum(new_cnt),count(distinct device_key) 
from rptdata.fact_kesheng_sdk_new_device_hourly t1 
where src_file_day>= 20170112 
group by src_file_day 
order by src_file_day;

-- 新增用户从17号开始忽然下降,为什么？

20170112	962178	253554	98654
20170113	979866	232172	100534
20170114	1046016	251872	108584
20170115	1047804	221942	108063
20170116	1029654	212053	108179
20170117	457446	76394	49888
20170118	337776	48137	30163
20170119	352278	51770	31551
20170120	370848	79693	34584
20170121	388158	68396	35417
20170122	396282	57415	35627
20170123	394560	49783	35274
20170124	96294	9606	12044



-- 原版 --
set mapreduce.job.name=stg.fact_kesheng_sdk_new_device_hourly_01_${SRC_FILE_DAY}_${SRC_FILE_HOUR};
set hive.groupby.skewindata=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.optimize.skewjoin=true;
set hive.merge.mapredfiles=true;

with deviceinfo as
(
SELECT
    case when t1.imei is null or t1.imei = '' or t1.imei = 'null' then '-998' else t1.imei end as imei,
    case when t1.user_id is null or t1.user_id = '' or t1.user_id = 'null' then '-998' else t1.user_id end as user_id,
    case when t1.idfa is null or t1.idfa = '' or t1.idfa = 'null' then '-998' else t1.idfa end as idfa,
    case when t1.imsi is null or t1.imsi = '' or t1.imsi = 'null' then '-998' else t1.imsi end as imsi,    
    t1.app_ver_code,
    t1.os,
    nvl(t2.product_key, -998) AS product_key,
    t1.first_launch_channel_id AS app_channel_id,
    t1.start_unix_time,
    t1.src_file_day,
    t1.src_file_hour
FROM intdata.kesheng_sdk_session_start t1 LEFT JOIN mscdata.dim_kesheng_sdk_app_pkg t2 ON (t1.os = t2.app_os_type AND t1.app_pkg_name = t2.app_pkg_name)
WHERE t1.src_file_day = '${SRC_FILE_DAY}'
      and t1.src_file_hour = '${SRC_FILE_HOUR}'
)
INSERT OVERWRITE TABLE stg.fact_kesheng_sdk_new_device_hourly_01
SELECT device_key, 
       app_channel_id, 
       product_key,
       app_ver_code, 
       min(start_unix_time) AS start_unix_time, 
       src_file_day, 
       src_file_hour
FROM (select t2.device_key, t1.* from deviceinfo t1 join intdata.kesheng_sdk_imei_user_device_key t2 on (t1.imei = t2.imei and t1.user_id = t2.user_id) where t1.os = 'AD' and t1.imei <> '-998' and t1.user_id <> '-998'
  union all
  select t2.device_key, t1.* from deviceinfo t1 join intdata.kesheng_sdk_imei_imsi_device_key t2 on (t1.imei  = t2.imei and t1.imsi = t2.imsi) where t1.os = 'AD' and t1.imei <> '-998' and t1.user_id = '-998' and t1.imsi <> '-998'
  union all
  select t2.device_key, t1.* from deviceinfo t1 join intdata.kesheng_sdk_idfa_user_device_key t2 on (t1.idfa = t2.idfa and t1.user_id = t2.user_id) where t1.os = 'iOS'
) t
GROUP BY device_key, app_channel_id, product_key, app_ver_code, src_file_day, src_file_hour;


-- 改进细节 --
INSERT OVERWRITE TABLE temp.fact_kesheng_sdk_new_device_hourly_01b
SELECT device_key, 
       t5.app_channel_id, 
       t5.product_key,
       t5.app_ver_code, 
       min(start_unix_time) AS start_unix_time, 
       '20170117' src_file_day, 
       '09' src_file_hour
FROM deviceinfo t5 inner join
       (select * from intdata.kesheng_sdk_active_device_hourly b2
         where b2.src_file_day = '20170117'
           and b2.src_file_hour = '09'
       ) t6
    on (t5.os = t6.app_os_type and t5.imei = t6.imei and t5.user_id = t6.user_id
          and t5.idfa = t6.idfa and t5.imsi = t6.imsi and t5.phone_number = t6.phone_number
        )
GROUP BY device_key, app_channel_id, product_key, app_ver_code;


case when t1.imsi is null or t1.imsi = '' or t1.imsi = 'null' then '-998' else t1.imsi end as imsi,    
case when t1.phone_number is null or t1.phone_number = '' or t1.phone_number = 'null' then '-998' else t1.phone_number end as   phone_number,
t1.app_ver_code,


-- 改进版 --
set mapreduce.job.name=stg.fact_kesheng_sdk_new_device_hourly_01_${SRC_FILE_DAY}_${SRC_FILE_HOUR};
set hive.merge.mapredfiles=true;

with deviceinfo as
(
SELECT
    case when t1.imei is null or t1.imei = '' or t1.imei = 'null' then '-998' else t1.imei end as imei,
    case when t1.user_id is null or t1.user_id = '' or t1.user_id = 'null' then '-998' else t1.user_id end as user_id,
    case when t1.idfa is null or t1.idfa = '' or t1.idfa = 'null' then '-998' else t1.idfa end as idfa,
    case when t1.imsi is null or t1.imsi = '' or t1.imsi = 'null' then '-998' else t1.imsi end as imsi,
	case when t1.phone_number is null or t1.phone_number = '' or t1.phone_number = 'null' then '-998' else t1.phone_number end as phone_number,    
    t1.app_ver_code,
    t1.os,
    nvl(t2.product_key, -998) AS product_key,
    t1.first_launch_channel_id AS app_channel_id,
    t1.start_unix_time,
    t1.src_file_day,
    t1.src_file_hour
FROM intdata.kesheng_sdk_session_start t1 LEFT JOIN mscdata.dim_kesheng_sdk_app_pkg t2 ON (t1.os = t2.app_os_type AND t1.app_pkg_name = t2.app_pkg_name)
WHERE t1.src_file_day = '${SRC_FILE_DAY}'
      and t1.src_file_hour = '${SRC_FILE_HOUR}'
)
INSERT OVERWRITE TABLE stg.fact_kesheng_sdk_new_device_hourly_01
SELECT device_key, 
       app_channel_id, 
       product_key,
       app_ver_code, 
       min(start_unix_time) AS start_unix_time, 
       src_file_day, 
       src_file_hour
FROM (
	select t2.device_key, t1.* 
     from deviceinfo t1 
     inner join (
				select * from intdata.kesheng_sdk_active_device_hourly
				where src_file_day = '${SRC_FILE_DAY}' and src_file_hour = '${SRC_FILE_HOUR}'
	            ) t2 
     on (t1.imei = t2.imei and t1.os = t2.app_os_type and t1.user_id = t2.user_id and t1.imsi = t2.imsi and t1.idfa = t2.idfa and t1.phone_number = t2.phone_number)
) t
GROUP BY device_key, app_channel_id, product_key, app_ver_code, src_file_day, src_file_hour;