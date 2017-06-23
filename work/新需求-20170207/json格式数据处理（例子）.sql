
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
TBLPROPERTIES (
'last_modified_by'='hadoop', 
'last_modified_time'='1485153712', 
'transient_lastDdlTime'='1485153712')

show partitions ods.kesheng_sdk_json_ex;
select * from ods.kesheng_sdk_json_ex where extract_date_label='20170123' and extract_hour_label = '01' limit 10;

{"sdkSessionInfo":{"imei":"de09db3dd67148c38d4de7ab13304529"
					,"udid":"C14AAC91-078F-4651-A24C-13B46FDF5E3E"
					,"idfa":"00000000-0000-0000-0000-000000000000"
					,"idfv":"96C375D3-9BA1-47C4-9985-6DF0E2086B1E"
					,"appVersion":"4.0.0"
					,"networkType":"WiFi"
					,"apppkg":"com.cmvideo.migumovie"
					,"clientId":"111111"
				  }
,"deviceInfo":{}
,"timeInfo":[]
,"eventInfo":[]
,"exception":[]
,"customInfo":[{ "account" :"13764291214"
				, "result" :"0"
				, "stateNumID" :"71"
				, "programeUrl" :"http:\/\/gslb.miguvod.lovev.com\/depository\/asset\/zhengshi\/5100\/057\/052\/5100057052\/media\/5100057052_5000721504_72.mp4.m3u8?msisdn=13764291214&mdspid=&spid=600058&netType=4&sid=5500076378&pid=2028596353&timestamp=20170122231528&Channel_ID=0111_64040000-99000-700200000000008&ProgramID=621215982&ParentNodeID=-99&client_ip=101.87.161.234&assertID=5500076378&imei=b616bb404219c45b2e3b3aea0b35caf920f48bbe51b7a036904fad1bdd6475f8&SecurityKey=20170122231528&encrypt=05b8d5b72e0bc9332504e658d81b0f7a&jid=e1059983ee3d1fd4313e3c1c4ba474bd"
				, "rateType" :"50"
				, "ContentID" :"621215982"
				, "programeType" :"1"
				, "type" :"17"
				, "timestamp" :"2017-01-23 00:59:46:182"
				, "state" :"Buffering"
				, "playSessionID" :"e1059983ee3d1fd4313e3c1c4ba474bd"
			   }
			  ,{ "networkType2" :"WIFI"
				  , "result" :"0"
				  , "dns" :""
				  , "account" :"13764291214"
				  , "mask" :""
				  , "type" :"2"
				  , "timestamp" :"2017-01-23 00:59:46"
				  , "ip" :"10.83.198.154"
				  , "networkType" :"WIFI"
			   },
			   { "account" :"13764291214"
				 , "result" :"0"
				 , "size" :"314094592"
				 , "ContentID" :"621215982"
				 , "type" :"26"
				 , "timestamp" :"2017-01-23 00:59:46:186"
				 , "playSessionID" :"e1059983ee3d1fd4313e3c1c4ba474bd"
			   }
			 ]
,"clientInfo":{}
}	
101.87.161.234	
20170123	
01

####################################################################################################################

CREATE VIEW `ods.kesheng_sdk_json_custominfo_v` AS SELECT concat(`s`.`input__file__name`, ':', `s`.`block__offset__inside__file`) as `rowkey`,
       `c`.`custominfo_pos`,
       `c`.`custominfo_json`,
       `c1`.`custominfo_type`,
       `s`.`extract_date_label`,
       `s`.`extract_hour_label`
FROM `ods`.`kesheng_sdk_json_ex` `s`
LATERAL VIEW json_tuple(translate(`s`.`json`, '\u0000', ''), 'customInfo') `p` as `customInfo`
LATERAL VIEW posexplode(split(regexp_replace(regexp_replace(`p`.`custominfo`,'\\}\\,\\{','\\}\\|\\|\\{'),'\\[|\\]',''), '\\|\\|')) `c` as `custominfo_pos`, `customInfo_json`
LATERAL VIEW json_tuple(`c`.`custominfo_json`, 'type') `c1` as `custominfo_type`
WHERE `c1`.`custominfo_type` IS NOT NULL
-------代码分析-------------------------------------------
\u0000代表的是NULL,输出控制台是一个空格

正则表达式：
'\\}\\,\\{': 匹配 '},{'
'\\}\\|\\|\\{': 匹配 '}||{'
'\\[|\\]': 匹配'['或']'
'\\|\\|': 匹配'||'
regexp_replace(`p`.`custominfo`,'\\}\\,\\{','\\}\\|\\|\\{'):把custominfo中的 '},{' 替换为 '}||{';
再把 '['或']' 替换为 '';
再通过'||'来分割,返回的是array<string>类型;
posexplode(a) - behaves like explode for arrays, but includes the position of items in the original array

explode(a) - separates the elements of array a into multiple rows, or the elements of a map into multiple rows and columns

-- ======================================================================================= --
INSERT OVERWRITE TABLE odsdata.kesheng_sdk_json_custominfo PARTITION(extract_date_label, extract_hour_label)
  SELECT 
        rowkey,
        custominfo_pos,
        customInfo_json,
        custominfo_type,
        extract_date_label,
        extract_hour_label
FROM ods.kesheng_sdk_json_custominfo_v
WHERE extract_date_label = '${EXTRACT_DATE}'
      AND extract_hour_label = '${EXTRACT_HOUR}';
	  
####################################################################################################################

CREATE VIEW `ods.kesheng_sdk_json_sessioninfo_v` AS SELECT concat(`s`.`input__file__name`, ':', `s`.`block__offset__inside__file`) as `rowkey`,
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
       `c`.`custominfo_pos`,
       `c`.`custominfo_json`,
       `c1`.`custominfo_type`,
       `s`.`client_ip`,
       `s`.`extract_date_label` as `src_file_day`,
       `s`.`extract_hour_label` as `src_file_hour`
FROM `ods`.`kesheng_sdk_json_ex` `s`
LATERAL VIEW json_tuple(`s`.`json`, 'sdkSessionInfo') `p` as `sdkSessionInfo`
LATERAL VIEW json_tuple(`p`.`sdksessioninfo`, 'clientId', 'imei', 'udid', 'idfa', 'idfv', 'appVersion', 'apppkg', 'networktype', 'os', 'appchannel', 'installationID') `d` AS `clientId`, `imei`, `udid`, `idfa`, `idfv`, `appVersion`, `apppkg`, `networktype`, `os`, `appchannel`, `installationID`
LATERAL VIEW json_tuple(translate(`s`.`json`, '\u0000', ''), 'customInfo') `p` as `customInfo`
LATERAL VIEW posexplode(split(regexp_replace(regexp_replace(`p`.`custominfo`,'\\}\\,\\{','\\}\\|\\|\\{'),'\\[|\\]',''), '\\|\\|')) `c` as `custominfo_pos`, `customInfo_json`
LATERAL VIEW json_tuple(`c`.`custominfo_json`, 'type') `c1` as `custominfo_type`
WHERE `c1`.`custominfo_type` IS NOT NULL AND LENGTH(`c1`.`custominfo_type`) <= 255

-- ============================================================================================= --
INSERT OVERWRITE TABLE odsdata.kesheng_sdk_sessioninfo PARTITION(src_file_day, custominfo_type)
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
      v.custominfo_pos,
      v.customInfo_json,
      v.src_file_day,
      v.custominfo_type
FROM ods.kesheng_sdk_json_sessioninfo_v v LEFT JOIN mscdata.ip_city_provider_map e ON (regexp_extract(v.client_ip, '(.*)(\\..*)', 1) = e.ip_segment)
WHERE src_file_day = '${EXTRACT_DATE}';
	  
####################################################################################################################

CREATE VIEW ods.kesheng_sdk_sessionstart_v 
AS 
SELECT concat(s.input__file__name, ':', s.block__offset__inside__file) as rowkey,
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
`s`.`extract_date_label` as `src_file_day`,
`s`.`extract_hour_label` as `src_file_hour`
FROM ods.kesheng_sdk_json_ex s		-- 外部表 (json 表示该表中一行为Json格式)
LATERAL VIEW json_tuple(s.json, 'sessionStart') p as sessionStart	--是hive针对json数据格式解析的函数，即json_tuple(…)，还有一个是get_json_object（…）
LATERAL VIEW json_tuple(p.sessionstart, 'os', 
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
'clientId') d
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
WHERE `p`.`sessionstart` is not null and `p`.`sessionstart` <> ''

-- ====================================================================================== --
INSERT OVERWRITE TABLE intdata.kesheng_sdk_session_start PARTITION(src_file_day, src_file_hour)	-- 字段存在null或者空字符串的情况
  SELECT 
        concat_ws('_', nvl(installationID, ''),  case when v.os = 'iOS' then  nvl(v.idfa, '') when v.os = 'AD' then nvl(v.imei, '') end) as device_id,
		-- device_key 的形式：IOS对应installationID_idfa 或者 AD对应installationID_imei
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
		-- 等价于 split(v.firstLaunchChannelId, '-')['-'的个数]   
		--有以“-”分成的三段或只有一段；如果有三段则取最后一段；
        v.userId,
        v.account,
        cast (v.startTs as bigint) as start_unix_time,	--  这个字段注意下！！  首次显示时间点时间,1970到目前为止的时间戳
        v.clientId,
        v.src_file_day,
        v.src_file_hour
FROM ods.kesheng_sdk_sessionstart_v v
WHERE  ((v.os = 'AD' and (v.installationID is not null or v.imei is not null)) 
          or (v.os = 'iOS' and (v.installationID is not null or v.idfa is not null)))
      and v.src_file_day = '${EXTRACT_DATE}'
      and v.src_file_hour = '${EXTRACT_HOUR}';

####################################################################################################################

CREATE VIEW `ods.kesheng_sdk_deviceinfo_v` AS SELECT concat(`s`.`input_file_name`, ':', `s`.`block_offset_inside_file`) as `rowkey`,
       `d`.`os`,
       `d`.`imei`,
       `d`.`imsi`,
       `d`.`idfa`,
       `d`.`idfv`,
       `d`.`sim`,
       `d`.`macid`,
       `d`.`appversion`,
       `d`.`phonemode`,
       `d`.`phonebrand`,
       `d`.`phonenumber`,
       `d`.`sreenwidth`,
       `d`.`sreenheight`,
       `d`.`sreendensity`,
       `d`.`appkey`,
       `d`.`apppkg`,
       `d`.`appchannel`,
       `d`.`userid`,
       `d`.`language`,
       `d`.`isbtb`,
       `d`.`backcount`,
       `d`.`uploadts`,
       `d`.`clientid`,
       `d`.`sessionid`,
       `d`.`osversion`,
       `d`.`sdkversion`,
       `d`.`ints`,
       `d`.`networktype`,
       `d`.`udid`,
       `d`.`installationid`,
       `s`.`client_ip`,
       `s`.`extract_date_label`,
       `s`.`extract_hour_label`
FROM `ods`.`kesheng_sdk_json_ex_v` `s`
LATERAL VIEW json_tuple(`s`.`json`, 'deviceInfo') `p` as `deviceInfo`
LATERAL VIEW json_tuple(`p`.`deviceinfo`, 'os', 'imei', 'imsi', 'idfa', 'idfv', 'sim', 'macId', 'appVersion', 'phoneMode', 'phoneBrand', 'phoneNumber', 'sreenWidth', 'sreenHeight', 'sreenDensity', 'appkey', 'apppkg', 'appchannel', 'userid', 'language', 'isBTB', 'backCount', 'uploadTs', 'clientId', 'sessionId', 'osversion', 'sdkversion', 'ints', 'networktype', 'udid', 'installationID') `d` AS `os`, `imei`, `imsi`, `idfa`, `idfv`, `sim`, `macId`, `appVersion`, `phoneMode`, `phoneBrand`, `phoneNumber`, `sreenWidth`, `sreenHeight`, `sreenDensity`, `appkey`, `apppkg`, `appchannel`, `userid`, `language`, `isBTB`, `backCount`, `uploadTs`, `clientId`, `sessionId`, `osversion`, `sdkversion`, `ints`, `networktype`, `udid`, `installationID`
WHERE `p`.`deviceinfo` IS NOT NULL;

-- ====================================================================================== --
INSERT OVERWRITE TABLE odsdata.kesheng_sdk_deviceinfo PARTITION(extract_date_label, extract_hour_label)
  SELECT 
        v.rowkey,
        v.os,
        v.imei,
        v.imsi,
        v.idfa,
        v.idfv,
        v.sim,
        v.macId,
        v.appVersion,
        v.phoneMode,
        v.phoneBrand,
        v.phoneNumber,
        v.sreenWidth,
        v.sreenHeight,
        v.sreenDensity,
        v.appkey,
        v.apppkg,
        v.appchannel,
        v.userid,
        v.language,
        v.isBTB,
        v.backCount,
        v.uploadTs,
        v.clientId,
        v.sessionId,
        v.osversion,
        v.sdkversion,
        v.ints,
        v.networktype,
        n.networktype1,
        n.networktype2,
        v.client_ip,
        e.province_name,
        e.city_name,
        e.provider_name,
        v.extract_date_label,
        v.extract_hour_label
FROM ods.kesheng_sdk_deviceinfo_v v LEFT JOIN mscdata.ip_city_provider_map e ON (regexp_extract(v.client_ip, '(.*)(\\..*)', 1) = e.ip_segment)
                                    LEFT JOIN ods.kesheng_sdk_network_v n ON (v.rowkey = n.rowkey and v.extract_date_label = n.extract_date_label and v.extract_hour_label = n.extract_hour_label)
WHERE v.extract_date_label = '${EXTRACT_DATE}'
      and v.extract_hour_label = '${EXTRACT_HOUR}';

	  