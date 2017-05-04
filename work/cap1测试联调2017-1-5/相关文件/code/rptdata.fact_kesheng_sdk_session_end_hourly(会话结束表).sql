`ods`.`kesheng_sdk_json_v` (sessionEnd部分) |->  ods.kesheng_sdk_sessionend_v |-> intdata.kesheng_sdk_session_end  |-> rptdata.fact_kesheng_sdk_session_end_daily

set mapreduce.job.name=rptdata.fact_kesheng_sdk_session_end_daily_${EXTRACT_DATE};
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

-- 每天session结束用户数据轻度汇总
insert overwrite table rptdata.fact_kesheng_sdk_session_end_daily partition(src_file_day='${EXTRACT_DATE}')
select nvl(t2.device_key,-998), t1.imei, t1.user_id, t1.idfa, t1.imsi, t1.app_ver_code
      ,t1.app_pkg_name, t1.phone_number, t1.app_os_type
      ,t1.first_launch_channel_id, t1.app_channel_id
      ,t1.product_key, t1.device_id, sum(t1.duration_ms) as duration_ms
  from (select if(nvl(trim(a1.imei),'') = '', '-998', trim(a1.imei)) imei
              ,if(nvl(trim(a1.user_id),'') = '', '-998', trim(a1.user_id)) user_id
              ,if(nvl(trim(a1.idfa),'') = '', '-998', trim(a1.idfa)) idfa
              ,if(nvl(trim(a1.imsi),'') = '', '-998', trim(a1.imsi)) imsi
              ,a1.app_ver_code ,a1.app_pkg_name
              ,if(nvl(trim(a1.phone_number),'') = '', '-998', trim(a1.phone_number)) phone_number 
              ,a1.os as app_os_type
              ,a1.first_launch_channel_id
              ,nvl(trim(regexp_extract(a1.first_launch_channel_id,'([^-]+$)',1)),'-998') app_channel_id
              ,nvl(d1.product_key,-998) product_key, a1.duration_ms, '' as device_id
          from intdata.kesheng_sdk_session_end a1
		  left join  mscdata.dim_kesheng_sdk_app_pkg d1
		  on  d1.app_os_type = a1.os and d1.app_pkg_name = a1.app_pkg_name
         where a1.src_file_day = '${EXTRACT_DATE}' and a1.duration_ms > 0
       ) t1
  left join
       (select * from intdata.kesheng_sdk_active_device_hourly b2
         where b2.src_file_day = '${EXTRACT_DATE}'
       ) t2
    on (t1.imei = t2.imei and t1.user_id = t2.user_id and t1.idfa = t2.idfa 
        and t1.imsi = t2.imsi and t1.phone_number = t2.phone_number)
 group by nvl(t2.device_key,-998), t1.imei, t1.user_id, t1.idfa, t1.imsi, t1.app_ver_code
             ,t1.app_pkg_name, t1.phone_number, t1.app_os_type
             ,t1.first_launch_channel_id, t1.app_channel_id
             ,t1.product_key, t1.device_id;
			 
-- ================================================================================			 
			 
add jar /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;
set mapreduce.job.name=intdata.kesheng_sdk_session_end_${EXTRACT_DATE}_${EXTRACT_HOUR};
set hive.enforce.bucketing=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

INSERT OVERWRITE TABLE intdata.kesheng_sdk_session_end PARTITION(src_file_day='${EXTRACT_DATE}', src_file_hour='${EXTRACT_HOUR}')
  SELECT 
        concat_ws('_', nvl(installationID, ''),  case when v.os = 'iOS' then  nvl(v.idfa, '') when v.os = 'AD' then nvl(v.imei, '') end) as device_id,
        v.os,
        v.imei,
        v.imsi,
        v.appPackageName,
        v.idfa,
        v.idfv,
        v.currentAppVersionCode,
        v.currentOSVersion,
        v.currentAppVersionName,
        v.phoneNumber,
        v.installationID,
        split(v.firstLaunchChannelId, '-')[length(v.firstLaunchChannelId) - length(translate(v.firstLaunchChannelId, '-', ''))] as first_launch_channel_id,
        v.userId,
        v.account,
        cast (v.endTs as bigint) as end_unix_time,
        cast (duration as bigint) as duration_ms,
        openUDID,
        v.clientId
FROM ods.kesheng_sdk_sessionend_v v
WHERE v.src_file_day = '${EXTRACT_DATE}'
      and v.src_file_hour = '${EXTRACT_HOUR}';
	  
-- ================================================================================	
	  
CREATE VIEW `ods.kesheng_sdk_sessionend_v` AS SELECT concat(`s`.`input_file_name`, ':', `s`.`block_offset_inside_file`) as `rowkey`,
       `d`.`os`,
       `d`.`imei`,
       `d`.`imsi`,
       `d`.`apppackagename`,
       `d`.`idfa`,
       `d`.`idfv`,
       `d`.`currentappversioncode`,
       `d`.`currentosversion`,
       `d`.`currentappversionname`,
       `d`.`phonenumber`,
       `d1`.`installationid`,
       `d`.`firstlaunchchannelid`,
       `d`.`userid`,
       `d`.`account`,
       `d`.`endts`,
       `d`.`duration`,
       `d`.`openudid`,
       `d`.`clientid`,
       `s`.`src_file_day` ,
       `s`.`src_file_hour`
FROM `ods`.`kesheng_sdk_json_v` `s`
LATERAL VIEW json_tuple(`s`.`json`, 'sdkSessionInfo') `p1` as `sdkSessionInfo`
LATERAL VIEW json_tuple(`p1`.`sdksessioninfo`, 'installationID') `d1` AS `installationID`
LATERAL VIEW json_tuple(`s`.`json`, 'sessionEnd') `p` as `sessionEnd`
LATERAL VIEW json_tuple(`p`.`sessionend`, 'os', 
                                      'imei', 
                                      'imsi', 
                                      'appPackageName', 
                                      'idfa', 
                                      'idfv', 
                                      'currentAppVersionCode', 
                                      'currentOSVersion', 
                                      'currentAppVersionName', 
                                      'phoneNumber', 
                                      'firstLaunchChannelId', 
                                      'userId', 
                                      'account', 
                                      'endTs', 
                                      'duration',
                                      'openUDID',
                                      'clientId') `d` 
                                      AS `os`, 
                                         `imei`, 
                                         `imsi`, 
                                         `appPackageName`, 
                                         `idfa`, 
                                         `idfv`, 
                                         `currentAppVersionCode`, 
                                         `currentOSVersion`, 
                                         `currentAppVersionName`, 
                                         `phoneNumber`, 
                                         `firstLaunchChannelId`, 
                                         `userId`, 
                                         `account`, 
                                         `endTs`,
                                         `duration`,
                                         `openUDID`,
                                         `clientId`
WHERE `p`.`sessionend` is not null and `p`.`sessionend` <> ''


	  
