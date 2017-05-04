`ods`.`kesheng_sdk_json_v` (exception部分)|-> ods.kesheng_sdk_exception_v |-> odsdata.kesheng_sdk_exception |-> intdata.kesheng_sdk_exception		     |->	rptdata.fact_kesheng_sdk_exception_daily
																						           (取device_key)intdata.kesheng_sdk_active_device_hourly|

-- ============================================================================================================

set mapreduce.job.name=rptdata.fact_kesheng_sdk_exception_daily_${EXTRACT_DATE};
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

-- 每天异常用户数据轻度汇总
insert overwrite table rptdata.fact_kesheng_sdk_exception_daily partition(src_file_day='${EXTRACT_DATE}')
select nvl(t2.device_key,-998), t1.imei, t1.user_id, t1.idfa, t1.imsi, t1.app_ver_code
      ,t1.app_pkg_name, t1.src_app_channel_id, t1.phone_number, t1.app_os_type
      ,t1.first_launch_channel_id, t1.app_channel_id
	  ,t1.reason
      ,t1.product_key
	  ,count(1) as exception_cnt 
  from (select if(nvl(trim(a1.imei),'') = '', '-998', trim(a1.imei)) as imei
              ,if(nvl(trim(a1.userid),'') = '', '-998', trim(a1.userid)) as user_id
              ,if(nvl(trim(a1.idfa),'') = '', '-998', trim(a1.idfa)) as idfa
              ,if(nvl(trim(a1.imsi),'') = '', '-998', trim(a1.imsi)) as imsi
              ,a1.appversion as app_ver_code ,a1.apppkg as app_pkg_name, a1.appchannel as src_app_channel_id
              ,if(nvl(trim(a1.phonenumber),'') = '', '-998', trim(a1.phonenumber)) as phone_number 
              ,a1.os as app_os_type
              --,a1.first_launch_channel_id
              --,nvl(trim(regexp_extract(a1.first_launch_channel_id,'([^-]+$)',1)),'-998') app_channel_id
              ,'' first_launch_channel_id
              ,nvl(trim(regexp_extract(appchannel,'([^-]+$)',1)),'-998') as app_channel_id
              ,nvl(d1.product_key,-998) as product_key
			  , a1.reason
          from intdata.kesheng_sdk_exception a1
		  left join mscdata.dim_kesheng_sdk_app_pkg d1
             on d1.app_os_type = a1.os and d1.app_pkg_name = a1.apppkg
         where a1.extract_date_label = '${EXTRACT_DATE}'
       ) t1
  left join
       (select * from intdata.kesheng_sdk_active_device_hourly b2
         where b2.src_file_day = '${EXTRACT_DATE}'
       ) t2
    on (t1.imei = t2.imei and t1.user_id = t2.user_id and t1.idfa = t2.idfa 
        and t1.imsi = t2.imsi and t1.phone_number = t2.phone_number)
 group by nvl(t2.device_key,-998), t1.imei, t1.user_id, t1.idfa, t1.imsi, t1.app_ver_code
             ,t1.app_pkg_name, t1.src_app_channel_id, t1.phone_number, t1.app_os_type
             ,t1.first_launch_channel_id, t1.app_channel_id
             ,t1.product_key, t1.reason;

-- ================================================================================	

set mapreduce.job.name=intdata.kesheng_sdk_exception_${EXTRACT_DATE};
set hive.enforce.bucketing=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

INSERT OVERWRITE TABLE intdata.kesheng_sdk_exception PARTITION(extract_date_label)
  SELECT 
        os,
        imei,
        null as imsi,
        idfa,
        idfv,
        null as sim,
        null as macId,
        appVersion,
        null as phoneMode,
        null as phoneBrand,
        null as phoneNumber,
        null as sreenWidth,
        null as sreenHeight,
        null as sreenDensity,
        null as appkey,
        apppkg,
        appchannel,
        null as userid,
        null as language,
        null as isBTB,
        null as backCount,
        null as uploadTs,
        clientId,
        null as sessionId,
        null as osversion,
        null as sdkversion,
        null as ints,
        networktype,
        null as networktype1,
        null as networktype2,
        client_ip,
        province_name,
        city_name,
        provider_name,
       c.name,
       c.reason,
       c.ts,
         installationID,
         src_file_day
FROM odsdata.kesheng_sdk_exception c
WHERE src_file_day = '${EXTRACT_DATE}';

-- ================================================================================

add jar /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;
set mapreduce.job.name=odsdata.kesheng_sdk_exception_${EXTRACT_DATE};
set hive.enforce.bucketing=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
INSERT OVERWRITE TABLE odsdata.kesheng_sdk_exception PARTITION(src_file_day)
  SELECT 
        v.rowkey,
        v.clientId,
        v.imei,
        v.udid,
        v.idfa,
        v.idfv,
        v.appVersion,
        v.apppkg,
        v.networktype, 
        v.os,
        v.appchannel,
        v.installationID,
        v.client_ip,
        e.province_name,
        e.city_name,
        e.provider_name,
        v.exception_pos,
        v.name,
        v.reason,
        v.ts,
        v.src_file_day
FROM ods.kesheng_sdk_exception_v v 
LEFT JOIN mscdata.ip_city_provider_map e ON (regexp_extract(v.client_ip, '(.*)(\\..*)', 1) = e.ip_segment)
WHERE src_file_day = '${EXTRACT_DATE}';


-- (.*)(\\..*) ： 
-- ()  标记一个子表达式的开始和结束位置
-- .   任何字符，要匹配 . ，请使用 \.
-- *   匹配前面的子表达式零次或多次
select * from mscdata.ip_city_provider_map a limit 5;
+------------------+--------------+------------------+---------------+--+
| a.province_name  | a.city_name  | a.provider_name  | a.ip_segment  |
+------------------+--------------+------------------+---------------+--+
| 台湾               | 台北           | 中华电信             | 60.250.137    |
| 香港               |              |                  | 118.25.115    |
| 北京               | 北京           | 中电华通             | 101.238.179   |
| 河北               | 邢台           | 电信               | 222.223.106   |
| 广东               | 广州           | 联通               | 112.94.227    |
+------------------+--------------+------------------+---------------+--+
-- ================================================================================

CREATE VIEW `ods.kesheng_sdk_exception_v` AS SELECT `s`.`rowkey`,
       `d`.`clientid`,
       `d`.`imei`,
       `d`.`udid`,
       `d`.`idfa`,
       `d`.`idfv`,
       `d`.`appversion`,
       `d`.`apppkg`,
       `d`.`networktype`, 
       `d`.`os`,
       `d`.`appchannel`,
       `d`.`installationid`,
       `s`.`client_ip`,
       `c`.`exception_pos`,
       `c1`.`name`,
       `c1`.`reason`,
       `c1`.`ts`,
       `s`.`src_file_day` 
FROM `ods`.`kesheng_sdk_json_v` `s`
LATERAL VIEW json_tuple(`s`.`json`, 'sdkSessionInfo') `p` as `sdkSessionInfo`
LATERAL VIEW json_tuple(`p`.`sdksessioninfo`, 'clientId', 'imei', 'udid', 'idfa', 'idfv', 'appVersion', 'apppkg', 'networktype', 'os', 'appchannel', 'installationID') `d` AS `clientId`, `imei`, `udid`, `idfa`, `idfv`, `appVersion`, `apppkg`, `networktype`, `os`, `appchannel`, `installationID`
LATERAL VIEW json_tuple(`s`.`json`, 'exception') `p` as `exception`
LATERAL VIEW posexplode(split(regexp_replace(regexp_replace(`p`.`exception`,'\\}\\,\\{','\\}\\|\\|\\{'),'\\[|\\]',''), '\\|\\|')) `c` as `exception_pos`, `exception_json`
LATERAL VIEW json_tuple(`c`.`exception_json`, 'name', 'reason', 'ts') `c1` as `name`, `reason`, `ts`
WHERE `c`.`exception_json` <> ''
  and nvl(`c`.`exception_json`, '{}') <> '{}' and `c`.`exception_json` <> '[]'

