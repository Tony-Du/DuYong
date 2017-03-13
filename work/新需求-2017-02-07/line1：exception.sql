
-- == ods.kesheng_sdk_json_ex ======================================================

CREATE EXTERNAL TABLE `ods.kesheng_sdk_json_ex`(
  `json` string COMMENT 'from deserializer', 
  `client_ip` string COMMENT 'from deserializer')
PARTITIONED BY ( 
  `extract_date_label` string, 
  `extract_hour_label` string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '||' 
WITH SERDEPROPERTIES ( 
  'serialization.encoding'='UTF-8') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://ns1/user/hadoop/ods/kesheng' 

set hive.exec.rowoffset=true;
select client_ip, INPUT__FILE__NAME ,BLOCK__OFFSET__INSIDE__FILE ,ROW__OFFSET__INSIDE__BLOCK from ods.kesheng_sdk_json_ex limit 5; 
OK
+------------------+--------------------------------------------------------------------------+------------------------------+-----------------------------+--+
|    client_ip     |                            input__file__name                             | block__offset__inside__file  | row__offset__inside__block  |
+------------------+--------------------------------------------------------------------------+------------------------------+-----------------------------+--+
| 113.16.144.169   | hdfs://ns1/user/hadoop/ods/kesheng/20160811/00/kesheng.1470913703668.gz  | 0                            | 0                           |
| 202.101.229.130  | hdfs://ns1/user/hadoop/ods/kesheng/20160811/00/kesheng.1470913703668.gz  | 894                          | 0                           |
| 223.104.91.236   | hdfs://ns1/user/hadoop/ods/kesheng/20160811/00/kesheng.1470913703668.gz  | 1768                         | 0                           |
| 218.109.53.129   | hdfs://ns1/user/hadoop/ods/kesheng/20160811/00/kesheng.1470913703668.gz  | 2627                         | 0                           |
| 117.136.66.211   | hdfs://ns1/user/hadoop/ods/kesheng/20160811/00/kesheng.1470913703668.gz  | 5430                         | 0                           |
+------------------+--------------------------------------------------------------------------+------------------------------+-----------------------------+--+
  
-- == ods.kesheng_sdk_exception_v ===========================================================================

CREATE VIEW `ods.kesheng_sdk_exception_v` AS SELECT concat(`s`.`input__file__name`, ':', `s`.`block__offset__inside__file`) as `rowkey`,
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
       `s`.`extract_date_label` as `src_file_day` 
FROM `ods`.`kesheng_sdk_json_ex` `s`
LATERAL VIEW json_tuple(`s`.`json`, 'sdkSessionInfo') `p` as `sdkSessionInfo`
LATERAL VIEW json_tuple(`p`.`sdksessioninfo`, 'clientId', 'imei', 'udid', 'idfa', 'idfv', 'appVersion', 'apppkg', 'networktype', 'os', 'appchannel', 'installationID') `d` AS `clientId`, `imei`, `udid`, `idfa`, `idfv`, `appVersion`, `apppkg`, `networktype`, `os`, `appchannel`, `installationID`
LATERAL VIEW json_tuple(`s`.`json`, 'exception') `p` as `exception`
LATERAL VIEW posexplode(split(regexp_replace(regexp_replace(`p`.`exception`,'\\}\\,\\{','\\}\\|\\|\\{'),'\\[|\\]',''), '\\|\\|')) `c` as `exception_pos`, `exception_json`
LATERAL VIEW json_tuple(`c`.`exception_json`, 'name', 'reason', 'ts') `c1` as `name`, `reason`, `ts`
WHERE `c`.`exception_json` <> ''

-- 分析 -------------------------------------------------------------------------------
regexp_replace(`p`.`exception`,'\\}\\,\\{','\\}\\|\\|\\{')	-- 把p.exception中的 "},{" 替换为 "}||{" 
--   |	指明两项之间的一个选择（或）。要匹配 |，需使用 \|
regexp_replace(regexp_replace(`p`.`exception`,'\\}\\,\\{','\\}\\|\\|\\{'),'\\[|\\]','') -- 把"[" 或"]" 替换为 空字符串
split(regexp_replace(regexp_replace(`p`.`exception`,'\\}\\,\\{','\\}\\|\\|\\{'),'\\[|\\]',''), '\\|\\|'))	-- 按照"||"分割，返回的是array<string>类型
posexplode(a) - behaves like explode for arrays, but includes the position of items in the original array
explode(a) - separates the elements of array a into multiple rows, or the elements of a map into multiple rows and columns


-- == odsdata.kesheng_sdk_exception ===========================================================================

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
-- 分析 ---------------------------------------------
()	标记一个子表达式的开始和结束位置。子表达式可以获取供以后使用。要匹配这些字符，请使用 \( 和 \)
.	任何字符，表示.字符本身,使用\.
*	匹配前面的子表达式零次或多次
regexp_extract(v.client_ip, '(.*)(\\..*)', 1)	-- 截取第一次出现类似于 1231@#dafaf.agfsd898&@这样的字符

-- == intdata.kesheng_sdk_exception ===========================================================================

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

-- == rptdata.fact_kesheng_sdk_exception_daily ===========================================================================

set mapreduce.job.name=rptdata.fact_kesheng_sdk_exception_daily_${EXTRACT_DATE};
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

-- 每天异常用户数据轻度汇总
insert overwrite table rptdata.fact_kesheng_sdk_exception_daily partition(src_file_day='${EXTRACT_DATE}')
select nvl(t2.device_key,-998)
	  ,t1.imei
	  ,t1.user_id
	  ,t1.idfa
	  ,t1.imsi
	  ,t1.app_ver_code
      ,t1.app_pkg_name
	  ,t1.src_app_channel_id
	  ,t1.phone_number
	  ,t1.app_os_type
      ,t1.first_launch_channel_id
	  ,t1.app_channel_id
	  ,t1.reason
      ,t1.product_key
	  ,count(1) as exception_cnt
  from (select if(nvl(trim(a1.imei),'') = '', '-998', trim(a1.imei)) imei
              ,if(nvl(trim(a1.userid),'') = '', '-998', trim(a1.userid)) user_id
              ,if(nvl(trim(a1.idfa),'') = '', '-998', trim(a1.idfa)) idfa
              ,if(nvl(trim(a1.imsi),'') = '', '-998', trim(a1.imsi)) imsi
              ,a1.appversion app_ver_code 
			  ,a1.apppkg app_pkg_name
			  ,a1.appchannel src_app_channel_id
              ,if(nvl(trim(a1.phonenumber),'') = '', '-998', trim(a1.phonenumber)) phone_number 
              ,a1.os as app_os_type
              --,a1.first_launch_channel_id
              --,nvl(trim(regexp_extract(a1.first_launch_channel_id,'([^-]+$)',1)),'-998') app_channel_id
              ,'' first_launch_channel_id
              ,nvl(trim(regexp_extract(appchannel,'([^-]+$)',1)),'-998') app_channel_id
              ,nvl(d1.product_key,-998) product_key
			  ,a1.reason
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