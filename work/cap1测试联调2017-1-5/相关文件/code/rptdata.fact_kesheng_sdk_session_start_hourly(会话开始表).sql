
整体框架：
`ods`.`kesheng_sdk_raw_ex` |-> `ods`.`kesheng_sdk_json_v` (sessionStart模块) |-> ods.kesheng_sdk_sessionstart_v 
|-> intdata.kesheng_sdk_session_start |-> int.kesheng_sdk_session_start_cpa_v      |-> rptdata.fact_kesheng_sdk_session_start_hourly(启动次数)
							(取device_key)intdata.kesheng_sdk_active_device_hourly |
										

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

INSERT OVERWRITE TABLE intdata.kesheng_sdk_session_start PARTITION(src_file_day='${EXTRACT_DATE}', src_file_hour='${EXTRACT_HOUR}')
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
        cast (v.startTs as bigint) as start_unix_time,
        v.clientId
FROM ods.kesheng_sdk_sessionstart_v v
WHERE  ((v.os = 'AD' and (v.installationID is not null or v.imei is not null)) 
          or (v.os = 'iOS' and (v.installationID is not null or v.idfa is not null)))
      and v.src_file_day = '${EXTRACT_DATE}'
      and v.src_file_hour = '${EXTRACT_HOUR}';		 
		 
-- ############################################################################################################################################################################### --

CREATE VIEW `ods.kesheng_sdk_sessionstart_v` AS SELECT `s`.`rowkey`,
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
       `d`.`installationid`,
       `d`.`firstlaunchchannelid`,
       `d`.`userid`,
       `d`.`account`,
       `d`.`startts`,
       `d`.`clientid`,
       `s`.`src_file_day`,
       `s`.`src_file_hour`
FROM `ods`.`kesheng_sdk_json_v` `s`
LATERAL VIEW json_tuple(`s`.`json`, 'sessionStart') `p` as `sessionStart`
LATERAL VIEW json_tuple(`p`.`sessionstart`, 'os', 
                                      'imei', 
                                      'imsi', 
                                      'appPackageName', 
                                      'idfa', 
                                      'idfv', 
                                      'currentAppVersionCode', 
                                      'currentOSVersion', 
                                      'currentAppVersionName', 
                                      'phoneNumber', 
                                      'installationID', 
                                      'firstLaunchChannelId', 
                                      'userId', 
                                      'account', 
                                      'startTs', 
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
                                         `installationID`, 
                                         `firstLaunchChannelId`, 
                                         `userId`, 
                                         `account`, 
                                         `startTs`, 
                                         `clientId`
WHERE nvl(`p`.`sessionstart`, '{}') <> '{}' and `p`.`sessionstart` <> '[]'

-- ############################################################################################################################################################################### --										
										
CREATE VIEW `ods.kesheng_sdk_json_v` AS select regexp_replace(`k2`.`line`[0], '\\\\n|\\\\r|\\\\u001f|\\\\u0000', '') `json`, `k2`.`line`[1] `client_ip`
      ,`k2`.`src_file_day`
      ,`k2`.`src_file_hour`
      ,`k2`.`input_file_name` ,`k2`.`block_offset_inside_file`
      ,concat(`k2`.`input_file_name`,':',`k2`.`block_offset_inside_file`) `rowkey`
  from (select split(`k1`.`line`,'\\|\\|') `line`
              ,`k1`.`src_file_day` ,`k1`.`src_file_hour`
              ,`k1`.`input__file__name` `input_file_name`  ,`k1`.`block__offset__inside__file` `block_offset_inside_file`
          from `ods`.`kesheng_sdk_raw_ex` `k1`
       ) `k2`
	   

	   
`ods`.`kesheng_sdk_raw_ex`
hdfs://ns1/user/hadoop/ods/kesheng
